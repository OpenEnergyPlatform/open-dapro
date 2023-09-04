import os
import shutil
import urllib.request as request

import geopandas as gpd
import pandas as pd
from cjio import cityjson
from dagster import asset
from shapely import Polygon

import docker
from energy_dagster.utils import utils


@asset(key_prefix="raw", group_name="raw_data", compute_kind="python")
def get_lod2_bavaria_in_cityjson(context) -> None:
    """First downloads the citygml files from the OpenData portal of bavaria,
    then converts it to cityjson files using the citygml4j/citygml-tools
    docker image.
    """
    number_files_conversion = 100
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


def delete_all_files_in_folder(directory):
    for file in os.listdir(directory):
        os.remove(os.path.join(directory, file))


def move_json_files(root, target):
    for file in os.listdir(root):
        if file.endswith(".json"):
            shutil.move(os.path.join(root, file), os.path.join(target, file))


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="raw_data",
    non_argument_deps={"get_lod2_bavaria_in_cityjson"},
    compute_kind="python",
)
def building_data_from_cityjson(context) -> gpd.GeoDataFrame:
    """Extracts building data from cityjson files and writes it to
    the database.
    """
    constants = utils.get_constants("lod2_bavaria")
    data_directory = os.path.join(
        utils.get_dagster_data_path(), constants["save_directory"], "json"
    )
    gdf = None
    for filename in os.listdir(data_directory):
        file_path = os.path.join(data_directory, filename)
        data = cityjson.load(file_path, transform=True)
        buildings = data.get_cityobjects(type=["building", "buildingpart"])
        (
            building_ids,
            building_volumes,
            building_roof_areas,
            building_envelope_areas,
            creation_dates,
            municipality_keys,
            floor_plan_updates,
            geometries,
        ) = ([], [], [], [], [], [], [], [])
        for building_id, building in buildings.items():
            try:
                building_attributes = get_building_data(building)
            except Exception:
                print("ERROR, this building failed:")
                print(building)
                continue
            building_ids.append(building_id)
            building_volumes.append(building_attributes["volume"])
            building_roof_areas.append(building_attributes["roof_surface"])
            building_envelope_areas.append(
                building_attributes["building_envelope_surface"]
            )
            creation_dates.append(building_attributes["creation_date"])
            municipality_keys.append(building_attributes["municipality_key"])
            floor_plan_updates.append(building_attributes["floor_plan_date"])
            geometries.append(building_attributes["ground_surface_polygon"])
        gdf_new = gpd.GeoDataFrame(
            {
                "id": building_ids,
                "municipality_key": municipality_keys,
                "creation_dates": creation_dates,
                "floor_plan_update": floor_plan_updates,
                "building_volume": building_volumes,
                "building_roof_area": building_roof_areas,
                "building_envelope_area": building_envelope_areas,
            },
            geometry=geometries,
            crs="25832",
        )
        gdf = gdf_new if gdf is None else pd.concat([gdf, gdf_new])
    return gdf


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
    roof_surface = calculate_roof_surface(building_geom)
    building_envelope_surface = calculate_building_envelope_surface(building_geom)

    creation_date = building_attributes["creationDate"][:10]
    municipality_key = building_attributes["Gemeindeschluessel"]
    floor_plan_date = building_attributes["Grundrissaktualitaet"]
    ground_surface_polygon = get_ground_surface_geometry(building_geom)

    return {
        "volume": volume,
        "roof_surface": roof_surface,
        "creation_date": creation_date,
        "municipality_key": municipality_key,
        "floor_plan_date": floor_plan_date,
        "ground_surface_polygon": ground_surface_polygon,
        "building_envelope_surface": building_envelope_surface,
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


def calculate_roof_surface(building_geom) -> float:
    """Calculates the total surface of all areas marked as roofs.

    Parameters
    ----------
    building_geom

    Returns
    -------
    float
        Roof surface in m^2
    """

    roof_surface_list = [
        surface
        for surface in building_geom.surfaces.values()
        if surface["type"] == "RoofSurface"
    ]
    return round(
        sum(float(surface["attributes"]["Flaeche"]) for surface in roof_surface_list)
    )


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
