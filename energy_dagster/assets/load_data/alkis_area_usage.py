import os

import fiona
import geopandas as gpd
from dagster import asset

from energy_dagster.utils import utils


@asset(
    key_prefix="raw",
    group_name="raw_data",
    compute_kind="python",
)
def load_alkis(context) -> None:
    constants = utils.download_from_constants("alkis_area_usage")
    filepath = os.path.join(constants["download_path"])
    if_exists = "replace"
    engine = utils.get_engine()
    schema, table_name = context.asset_key_for_output()[-1]

    for layer in fiona.listlayers(filepath):
        context.log.info(layer)
        gdf = gpd.read_file(filepath, layer=layer).to_crs(crs="EPSG:4326")
        gdf.to_postgis(name=table_name, con=engine, if_exists=if_exists, schema=schema)
        if_exists = "append"
