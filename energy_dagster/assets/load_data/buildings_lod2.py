import os
import shutil
import urllib.request as request

import geopandas as gpd
import pandas as pd
from cjio import cityjson
from dagster import AssetExecutionContext, asset
from shapely import Polygon

import docker
from energy_dagster.utils import utils


@asset(key_prefix="raw", group_name="raw_data", compute_kind="python")
def lod2_bavaria(context) -> None:
    number_files_conversion = 5
    client = docker.from_env()
    _ = client.images.pull(repository="citygml4j/citygml-tools")
    constants = utils.get_constants("lod2_bavaria")
    meta4_df = pd.read_xml(request.urlopen(constants["url"]).read())
    urls = meta4_df["url"]
    gml_directory = os.path.join(
        utils.get_dagster_data_path(), constants["save_directory"], "gml"
    )
    json_directory = os.path.join(
        utils.get_dagster_data_path(), constants["save_directory"], "json"
    )
    utils.create_directories([gml_directory, json_directory])

    for index, url in enumerate(urls):
        if (index + 1) % 500 == 0:
            context.log.info(f"Download progress: {round(index/len(urls) * 100):.0f}%")
        if not url:
            continue
        if not url.startswith("http"):
            continue
        filename = url.split("/")[-1]
        utils.download_from_url(url, save_directory=gml_directory, filename=filename)
        if (index + 1) % number_files_conversion != 0:
            # download number_files_conversion of gml files,
            # then convert them to json
            continue
        client.containers.run(
            "citygml4j/citygml-tools",
            command="to-cityjson *.gml --vertex-precision=10",
            volumes=[gml_directory.replace("\\", "/") + ":/data"],
            user=1000,
            auto_remove=True,
            name="citygml-tools-dagster",
        )
        move_json_files(root=gml_directory, target=json_directory)
        delete_all_files_in_folder(gml_directory)
        write_lod2_to_database(data_directory=json_directory, context=context)
        delete_all_files_in_folder(json_directory)
        return


def write_lod2_to_database(data_directory: str, context: AssetExecutionContext) -> None:
    engine = utils.get_engine()
    schema, table_name = context.asset_key_for_output()[-1]
    if_exists = "replace"
    for filename in os.listdir(data_directory):
        file_path = os.path.join(data_directory, filename)
        data = cityjson.load(file_path, transform=True)
        buildings = data.get_cityobjects(type=["building", "buildingpart"])
        building_ids = []
        columns = {
            "building_volume": [],
            "building_height": [],
            "roof_area_north": [],
            "roof_area_east": [],
            "roof_area_south": [],
            "roof_area_west": [],
            "roof_area_undefined": [],
            "building_envelope_area": [],
            "creation_date": [],
            "municipality_key": [],
            "floor_plan_update": [],
            "geometry": [],
        }
        for building_id, building in buildings.items():
            try:
                building_attributes = get_building_data(building)
            except Exception as e:
                print("ERROR, this building failed:")
                print(e)
                print(building)
                continue
            for column in columns.keys():
                columns[column].append(building_attributes[column])

            building_ids.append(building_id)
        columns["building_id"] = building_ids
        gdf = gpd.GeoDataFrame(
            columns,
            geometry="geometry",
            crs="25832",
        ).to_crs(crs="EPSG:4326")
        gdf.to_postgis(name=table_name, con=engine, if_exists=if_exists, schema=schema)
        context.log.info(
            f"Wrote {len(gdf)} to table {schema}.{table_name} in modus {if_exists}"
        )
        if_exists = "append"


def delete_all_files_in_folder(directory):
    for file in os.listdir(directory):
        os.remove(os.path.join(directory, file))


def move_json_files(root, target):
    for file in os.listdir(root):
        if file.endswith(".json"):
            shutil.move(os.path.join(root, file), os.path.join(target, file))


def get_building_data(building):
    """Extract relevant building data from a Cityjson building object.

    Parameters
    ----------
    building :
        Cityjson building object
    """
    building_geom = building.geometry[0]
    building_attributes = building.to_json()["attributes"]

    volume = calculate_building_volume(building_geom, building_attributes)
    roof_surfaces = calculate_roof_surface(building_geom)
    building_envelope_surface = calculate_building_envelope_surface(building_geom)

    creation_date = building_attributes["creationDate"][:10]
    municipality_key = building_attributes["Gemeindeschluessel"]
    floor_plan_date = building_attributes["Grundrissaktualitaet"]
    ground_surface_polygon = get_ground_surface_geometry(building_geom)

    return {
        "building_volume": volume,
        "building_height": building_attributes["measuredHeight"],
        "roof_area_north": roof_surfaces["north"],
        "roof_area_east": roof_surfaces["east"],
        "roof_area_south": roof_surfaces["south"],
        "roof_area_west": roof_surfaces["west"],
        "roof_area_undefined": roof_surfaces["undefined"],
        "building_envelope_area": building_envelope_surface,
        "creation_date": creation_date,
        "municipality_key": municipality_key,
        "floor_plan_update": floor_plan_date,
        "geometry": ground_surface_polygon,
    }


def calculate_building_envelope_surface(building_geom) -> float:
    """Calculate the total outer area of a building, e.g. the sum of all areas
    that are not a Ground surface.

    Parameters
    ----------
    building_geom

    Returns
    -------
    float
    The area of the building envelope in m^2

    """
    outer_surface_list = [
        surface
        for surface in building_geom.surfaces.values()
        if surface["type"] != "GroundSurface"
    ]
    return round(
        sum(float(surface["attributes"]["Flaeche"]) for surface in outer_surface_list)
    )


def calculate_roof_surface(building_geom) -> dict:
    """Calculates the total surface of all areas marked as roofs.

    Parameters
    ----------
    building_geom

    Returns
    -------
    dict
        Roof surface in different orientations
    """
    north, east, west, south, undefined = [], [], [], [], []
    roof_surface_list = [
        surface
        for surface in building_geom.surfaces.values()
        if surface["type"] == "RoofSurface"
    ]
    for surface in roof_surface_list:
        orientation = float(surface["attributes"]["Dachorientierung"])
        area = round(float(surface["attributes"]["Flaeche"]))
        if 45.0 <= orientation < 135.0:
            east.append(area)
        elif 135.0 <= orientation < 225.0:
            south.append(area)
        elif 225.0 <= orientation < 315.0:
            west.append(area)
        elif orientation >= 315.0 or orientation < 45.0:
            north.append(area)
        else:
            undefined.append(area)
    return {
        "north": sum(north),
        "east": sum(east),
        "west": sum(west),
        "south": sum(south),
        "undefined": sum(undefined),
    }

    return


def calculate_building_volume(building_geom, building_attributes) -> float:
    """Calculate the approximate volume of a building

    Parameters
    ----------
    building_geom

    Returns
    -------
    float
        Volume in m^3
    """
    ground_surface_list = [
        surface
        for surface in building_geom.surfaces.values()
        if surface["type"] == "GroundSurface"
    ]
    ground_surface = sum(
        float(surface["attributes"]["Flaeche"]) for surface in ground_surface_list
    )
    return round(
        ground_surface * float(building_attributes["measuredHeight"])
        + 0.5
        * ground_surface
        * (
            float(building_attributes["HoeheDach"])
            - float(building_attributes["NiedrigsteTraufeDesGebaeudes"])
        )
    )


def get_ground_surface_geometry(building_geom):
    """Returns the ground surface of a given building as a shapely Polygon."""

    building_surfaces = building_geom.get_surfaces()
    ground_surface_list = [
        surface
        for surface in building_geom.surfaces.values()
        if surface["type"] == "GroundSurface"
    ]
    ground_surface_idx = ground_surface_list[0]["surface_idx"][0]
    ground_surface = building_surfaces[ground_surface_idx[0]][ground_surface_idx[1]][0]
    ground_surface_points = [(point[0], point[1]) for point in ground_surface]
    return Polygon(ground_surface_points)
