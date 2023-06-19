from dagster import asset

import geopandas as gpd
from energy_dagster.utils import utils


def load_geoboundaries(area: str) -> gpd.GeoDataFrame:
    constants = utils.download_from_constants(area)
    download_path = constants["download_path"]
    zipfile_path = constants["zipfile_path"]
    zipfile = f"{download_path}!{zipfile_path}".replace("\\", "/").replace(
        "C:/", "zip:///"
    )
    return gpd.read_file(zipfile)


@asset(io_manager_key="postgis_io", key_prefix="raw", group_name="raw_data")
def districts() -> gpd.GeoDataFrame:
    """Download district boundary data from url defined in constants.yaml"""
    gdf = load_geoboundaries(area="geoboundaries_districts")
    return gdf


@asset(io_manager_key="postgis_io", key_prefix="raw", group_name="raw_data")
def municipalities() -> gpd.GeoDataFrame:
    """Download municipality boundary data from url defined in constants.yaml"""
    gdf = load_geoboundaries(area="geoboundaries_municipalities")
    return gdf
