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
    """
    Asset function to load OSM building data for the Oberpfalz region.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing OSM building data for the Oberpfalz region.

    Description
    -----------
    This asset function calls the 'load_osm_from_shp' function to load OSM building data specifically for the Oberpfalz region.
    The resulting GeoDataFrame is marked as an asset with specific metadata for PostGIS integration.
    """
    return load_osm_from_shp("osm_oberpfalz")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_schwaben():
    """
    Asset function to load OSM building data for the Schwaben region.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing OSM building data for the Schwaben region.

    Description
    -----------
    This asset function calls the 'load_osm_from_shp' function to load OSM building data specifically for the Schwaben region.
    The resulting GeoDataFrame is marked as an asset with specific metadata for PostGIS integration.
    """
    return load_osm_from_shp("osm_schwaben")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_unterfranken():
    """
    Asset function to load OSM building data for the Unterfranken region.
    """
    return load_osm_from_shp("osm_unterfranken")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_oberfranken():
    """
    Asset function to load OSM building data for the Oberfranken region.
    """
    return load_osm_from_shp("osm_oberfranken")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_mittelfranken():
    """
    Asset function to load OSM building data for the Mittelfranken region.
    """
    return load_osm_from_shp("osm_mittelfranken")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_oberbayern():
    """
    Asset function to load OSM building data for the Oberbayern region.
    """
    return load_osm_from_shp("osm_oberbayern")


@asset(
    io_manager_key="postgis_io",
    key_prefix="raw",
    group_name="osm",
    compute_kind="python",
)
def osm_niederbayern():
    """
    Asset function to load OSM building data for the Niederbayern region.
    """
    return load_osm_from_shp("osm_niederbayern")
