import os

import geopandas as gpd
from dagster import asset

from energy_dagster.utils import utils


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="raw_data",
    compute_kind="python",
)
def load_alkis() -> gpd.GeoDataFrame:
    constants = utils.download_from_constants("alkis_area_usage")
    filepath = os.path.join(constants["download_path"])
    return gpd.read_file(filepath)
