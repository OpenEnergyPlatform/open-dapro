import os
import urllib.request as request

import pandas as pd
from cjio import cityjson
from dagster import asset

import docker
from energy_dagster.utils import utils


def download_from_meta4(constants_key: str, context) -> None:
    """Generic function to download data where the urls are
    specified in a meta4 file

    Parameters
    ----------
    constants_key : str
        key of the constants yml file of the specific dataset

    """
    constants = utils.get_constants(constants_key)
    meta4_df = pd.read_xml(request.urlopen(constants["url"]).read())
    urls = meta4_df["url"]
    save_directory = os.path.join(
        utils.get_dagster_data_path(), constants["save_directory"]
    )

    number_files = len(urls)

    for index, url in enumerate(urls):
        if (index + 1) % 10 == 0:
            context.log.info(f"Download progress: {round(index/number_files): .0%}")
            break
        if not url:
            continue
        if url.startswith("http"):
            utils.download_from_url(
                url, save_directory=save_directory, filename=url.split("/")[-1]
            )


@asset(key_prefix="raw", group_name="raw_data")
def download_lod2_bavaria(context) -> None:
    """Downloads all citygml files for bavaria from the opendata
    portal.
    """
    download_from_meta4(constants_key="lod2_bavaria", context=context)


@asset(
    key_prefix="raw", group_name="raw_data", non_argument_deps={"download_lod2_bavaria"}
)
def convert_citygml_to_cityjson(context) -> None:
    """Converts all citygml files in the data_directory to cityjson files
    using the docker image from citygml4j/citygml-tools.
    """
    constants = utils.get_constants("lod2_bavaria")

    data_directory = os.path.join(
        utils.get_dagster_data_path(), constants["save_directory"]
    ).replace("\\", "/")
    client = docker.from_env()
    _ = client.images.pull(repository="citygml4j/citygml-tools")
    client.containers.run(
        "citygml4j/citygml-tools",
        command="to-cityjson *.gml",
        volumes=[f"{data_directory}:/data"],
        user=1000,
        auto_remove=True,
        name="citygml-tools-dagster",
    )


@asset(
    io_manager_key="db_io",
    key_prefix="raw",
    group_name="raw_data",
    non_argument_deps={"convert_citygml_to_cityjson"},
)
def building_data_from_cityjson(context) -> pd.DataFrame:
    """Extracts building data from cityjson files and writes it to
    the database.
    """
    constants = utils.get_constants("lod2_bavaria")
    data_directory = os.path.join(
        utils.get_dagster_data_path(), constants["save_directory"]
    )
    for filename in os.listdir(data_directory):
        if filename.endswith(".gml"):
            continue
        file_path = os.path.join(data_directory, filename)
        data = cityjson.load(file_path, transform=True)
        buildings = data.get_cityobjects(type=["building", "buildingpart"])

        building_ids = list(buildings.keys())
        (
            building_volumes,
            building_roof_areas,
            creation_dates,
            municipality_keys,
            floor_plan_updates,
        ) = ([], [], [], [], [])
        for _, building in buildings.items():
            (
                volume,
                roof_surface,
                creation_date,
                municipality_key,
                floor_plan_date,
            ) = get_building_data(building)

            building_volumes.append(volume)
            building_roof_areas.append(roof_surface)
            creation_dates.append(creation_date)
            municipality_keys.append(municipality_key)
            floor_plan_updates.append(floor_plan_date)
        return pd.DataFrame(
            list(
                zip(
                    building_ids,
                    municipality_keys,
                    creation_dates,
                    floor_plan_updates,
                    building_volumes,
                    building_roof_areas,
                )
            ),
            columns=[
                "id",
                "municipality_key",
                "creation_dates",
                "floor_plan_update",
                "building_volume",
                "building_roof_area",
            ],
        )


def get_building_data(building):
    """Extract relevant building data from a Cityjson building object.

    Parameters
    ----------
    building :
        Cityjson building object
    """
    building_geom = building.geometry[0]
    building_attributes = building.to_json()["attributes"]
    ground_suface_list = [
        surface
        for surface in building_geom.surfaces.values()
        if surface["type"] == "GroundSurface"
    ]
    roof_surface_list = [
        surface
        for surface in building_geom.surfaces.values()
        if surface["type"] == "RoofSurface"
    ]
    ground_surface = sum(
        float(surface["attributes"]["Flaeche"]) for surface in ground_suface_list
    )
    roof_surface = round(
        sum(float(surface["attributes"]["Flaeche"]) for surface in roof_surface_list)
    )
    volume = round(
        ground_surface * float(building_attributes["measuredHeight"])
        + 0.5
        * ground_surface
        * (
            float(building_attributes["HoeheDach"])
            - float(building_attributes["NiedrigsteTraufeDesGebaeudes"])
        )
    )
    creation_date = building_attributes["creationDate"][:10]
    municipality_key = building_attributes["Gemeindeschluessel"]
    floor_plan_date = building_attributes["Grundrissaktualitaet"]

    return volume, roof_surface, creation_date, municipality_key, floor_plan_date
