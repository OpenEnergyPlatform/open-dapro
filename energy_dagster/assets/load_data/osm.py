import geopandas as gpd
from dagster import asset

from energy_dagster.utils import utils


def load_osm_from_shp(area: str) -> gpd.GeoDataFrame:
    constants = utils.download_from_constants(area)
    download_path = constants["download_path"]
    print(download_path)
    zipfile = f"{download_path}!gis_osm_buildings_a_free_1.shp".replace(
        "\\", "/"
    ).replace("C:/", "zip:///")
    print(zipfile)
    return gpd.read_file(zipfile)


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_oberpfalz():
    return load_osm_from_shp("osm_oberpfalz")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_schwaben():
    return load_osm_from_shp("osm_schwaben")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_unterfranken():
    return load_osm_from_shp("osm_unterfranken")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_oberfranken():
    return load_osm_from_shp("osm_oberfranken")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_mittelfranken():
    return load_osm_from_shp("osm_mittelfranken")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_oberbayern():
    return load_osm_from_shp("osm_oberbayern")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_niederbayern():
    return load_osm_from_shp("osm_niederbayern")
