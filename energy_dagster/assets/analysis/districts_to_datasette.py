import os

from dagster import AssetIn, asset
from sqlalchemy import create_engine

from energy_dagster.utils import utils


# from dagster_dbt import get_asset_key_for_model
def replace_german_chars(input):
    if type(input) == "str":
        return (
            input.replace("ä", "ae")
            .replace("ö", "oe")
            .replace("ü", "ue")
            .replace("ß", "ss")
        )
    else:
        return input


@asset(
    group_name="analysis",
    compute_kind="python",
    ins={
        "marts_buildings": AssetIn(
            input_manager_key="dbt_marts_to_geopandas",
            key=("marts", "buildings"),
        )
    },
)
def buildings_to_datasette(context, marts_buildings) -> None:
    sqlite_path = os.path.join(utils.get_dagster_data_path(), "datasette.db")
    geojson_path = os.path.join(utils.get_dagster_data_path(), "datasette.json")
    # marts_buildings.to_sql(
    #    con=create_engine("sqlite:///" + sqlite_path), name="buildings"
    # )
    # marts_buildings.replace(
    #    to_replace=["ä", "ö", "ü", "ß"], value=["ae", "oe", "ue", "ss"], inplace=True
    # )
    # marts_buildings = marts_buildings.map(replace_german_chars)

    # print(marts_buildings.head())
    marts_buildings.drop(axis=1, labels="lod2_geometry", inplace=True)
    marts_buildings.astype({"mastr_updated_at": "str"}).to_file(
        filename=geojson_path, driver="GeoJSON"
    )
    context.log.info(len(marts_buildings))
