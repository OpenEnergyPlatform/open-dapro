import geopandas as gpd
from dagster import asset

from energy_dagster.utils import utils


def load_geoboundaries(area: str) -> gpd.GeoDataFrame:
    constants = utils.download_from_constants(area)
    download_path = constants["download_path"]
    zipfile_path = constants["zipfile_path"]
    zipfile = f"{download_path}!{zipfile_path}".replace("\\", "/").replace(
        "C:/", "zip:///"
    )
    return gpd.read_file(zipfile)


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="raw_data",
    compute_kind="python",
)
def districts_geoboundaries() -> gpd.GeoDataFrame:
    """Download district boundary data from url defined in constants.yaml

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame of the districts and their boundaries.
    """
    return load_geoboundaries(area="geoboundaries_districts")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="raw_data",
    compute_kind="python",
)
def municipalities_geoboundaries() -> gpd.GeoDataFrame:
    """Download municpality boundary data from url defined in constants.yaml

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame of the municipalities and their boundaries.
    """
    return load_geoboundaries(area="geoboundaries_municipalities")
