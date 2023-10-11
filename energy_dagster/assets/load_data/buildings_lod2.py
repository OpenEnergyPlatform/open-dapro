import os
import shutil
import stat
import urllib.request as request

import geopandas as gpd
import pandas as pd
import sqlalchemy
from cjio import cityjson
from dagster import AssetExecutionContext, asset
from shapely import Polygon
from sqlalchemy.sql import text

import docker
from energy_dagster.utils import utils


@asset(key_prefix="raw", group_name="raw_data", compute_kind="python")
def lod2_bavaria(context) -> None:
    is_develop_mode = False if os.environ.get("IS_DEVELOP_MODE") == "false" else True
    lod2_factory(
        region_name="lod2_nuernberger_land",
        context=context,
        is_develop_mode=is_develop_mode,
    )


def lod2_factory(
    region_name: str, context: AssetExecutionContext, is_develop_mode: bool
):
    number_files_conversion = 5
    client = docker.from_env()
    _ = client.images.pull(repository="citygml4j/citygml-tools")
    constants = utils.get_constants(region_name)
    meta4_df = pd.read_xml(request.urlopen(constants["url"]).read())
    urls = meta4_df["url"]
    gml_directory = os.path.join(
        utils.get_dagster_data_path(), constants["save_directory"], "gml"
    )
    json_directory = os.path.join(
        utils.get_dagster_data_path(), constants["save_directory"], "json"
    )
    utils.create_directories([gml_directory, json_directory])
    context.log.info(f"LOD2 files are downloaded to {gml_directory}")

    engine = utils.get_engine()
    delete_table_from_dagster_context(engine, context)
    delete_all_files_in_folder(gml_directory)

    for index, url in enumerate(urls):
        if (index + 1) % 50 == 0:
            context.log.info(f"Download progress: {round(index/len(urls) * 100):.0f}%")
        if not url:
            continue
        if not url.startswith("http"):
            continue
        filename = url.split("/")[-1]
        utils.download_from_url(url, save_directory=gml_directory, filename=filename)
        os.chmod(gml_directory, stat.S_IRWXO | stat.S_IRWXU | stat.S_IRWXG)

        if (index + 1) % number_files_conversion == 0:
            # download number_files_conversion of gml files,
            # then convert them to json

            if is_develop_mode:
                context.log.info("Running in dagster dev mode")
                client.containers.run(
                    "citygml4j/citygml-tools",
                    command="to-cityjson *.gml --vertex-precision=10",
                    volumes=[gml_directory.replace("\\", "/") + ":/data"],
                    user=1000,
                    auto_remove=True,
                    name="citygml-tools-dagster",
                )

            else:
                client.containers.run(
                    "citygml4j/citygml-tools",
                    command="to-cityjson lod2_nuernberger_land/gml/*.gml --vertex-precision=10",
                    volumes={
                        "dagster-docker_dagsterHomeData": {
                            "bind": "/data",
                            "mode": "rw",
                        }
                    },
                    user=1000,
                    network="docker_network",
                    auto_remove=True,
                    name="citygml-tools-dagster",
                )

            # execute_docker_container(
            #    context,
            #    image="citygml4j/citygml-tools",
            #    command="to-cityjson *.gml --vertex-precision=10",
            #    container_kwargs={
            #        "auto_remove": True,
            #        "volumes": gml_directory.replace("\\", "/") + ":/data",
            #    },
            # )
            move_json_files(root=gml_directory, target=json_directory)
            delete_all_files_in_folder(gml_directory)
            write_lod2_to_database(
                data_directory=json_directory, context=context, engine=engine
            )
            delete_all_files_in_folder(json_directory)
    delete_duplicated_buildings(engine=engine, context=context)


def delete_duplicated_buildings(engine, context):
    schema, table_name = context.asset_key_for_output()[-1]
    drop_table_sql = text(
        f""""
        WITH duplicates AS (
            SELECT building_id
            FROM {schema}.{table_name}
            GROUP BY building_id
            HAVING COUNT(*) > 1
        )
        DELETE FROM {schema}.{table_name}
        WHERE building_id IN (SELECT building_id FROM duplicates)
            AND ctid NOT IN (
                SELECT MIN(ctid) FROM {schema}.{table_name}
                WHERE building_id IN (SELECT building_id FROM duplicates)
                GROUP BY building_id
            );
        """
    )
    with engine.connect() as connection:
        connection.execute(drop_table_sql)
        connection.commit()


def delete_table_from_dagster_context(engine, context):
    schema, table_name = context.asset_key_for_output()[-1]
    drop_table_sql = text(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE")

    with engine.connect() as connection:
        connection.execute(drop_table_sql)
        connection.commit()
    context.log.info(f"Drop table {schema}.{table_name}")


def write_lod2_to_database(
    data_directory: str,
    context: AssetExecutionContext,
    engine: sqlalchemy.Engine,
    if_exists: str = "append",
) -> None:
    schema, table_name = context.asset_key_for_output()[-1]
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
        gdf = (
            gpd.GeoDataFrame(
                columns,
                geometry="geometry",
                crs="25832",
            )
            .to_crs(crs="EPSG:4326")
            .astype(
                dtype={
                    "creation_date": "str",
                    "municipality_key": "str",
                    "floor_plan_update": "str",
                    "building_id": "str",
                },
            )
        )
        gdf.to_postgis(name=table_name, con=engine, if_exists=if_exists, schema=schema)


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
