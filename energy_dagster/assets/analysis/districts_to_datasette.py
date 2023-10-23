import os

from dagster import AssetIn, asset

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
    marts_buildings.replace(
        to_replace=[
            "ä",
            "ö",
            "ü",
            "Ä",
            "Ö",
            "Ü",
            "ß",
            "ą",
            "ć",
            "ę",
            "ł",
            "ń",
            "ó",
            "ś",
            "ź",
            "ż",
            "á",
            "č",
            "ď",
            "é",
            "ě",
            "í",
            "ň",
            "ó",
            "ř",
            "š",
            "ť",
            "ú",
            "ů",
            "ý",
            "ž",
            "Á",
            "Č",
            "Ď",
            "É",
            "Ě",
            "Í",
            "Ň",
            "Ó",
            "Ř",
            "Š",
            "Ť",
            "Ú",
            "Ů",
            "Ý",
            "Ž",
            "Ą",
            "Ć",
            "Ę",
            "Ł",
            "Ń",
            "Ó",
            "Ś",
            "Ź",
            "Ż",
        ],
        value=[
            "ae",
            "oe",
            "ue",
            "Ae",
            "Oe",
            "Ue",
            "ss",
            "a",
            "c",
            "e",
            "l",
            "n",
            "o",
            "s",
            "z",
            "z",
            "a",
            "c",
            "d",
            "e",
            "e",
            "i",
            "n",
            "o",
            "r",
            "s",
            "t",
            "u",
            "u",
            "y",
            "z",
            "A",
            "C",
            "D",
            "E",
            "E",
            "I",
            "N",
            "O",
            "R",
            "S",
            "T",
            "U",
            "U",
            "Y",
            "Z",
            "A",
            "C",
            "E",
            "L",
            "N",
            "O",
            "S",
            "Z",
            "Z",
        ],
        inplace=True,
        regex=True,
    )
    # marts_buildings = marts_buildings.map(replace_german_chars)

    # print(marts_buildings.head())
    marts_buildings.drop(axis=1, labels="lod2_geometry", inplace=True)
    marts_buildings.astype({"mastr_updated_at": "str"}).to_file(
        filename=geojson_path, driver="GeoJSON"
    )
    context.log.info(len(marts_buildings))
    command = f"geojson-to-sqlite {sqlite_path} buildings_json {geojson_path}"
    # create_sqlite_db = create_shell_command_op(
    #    command,
    #    name="create_sqlite_db",
    # )
    # create_sqlite_db()
    create_sqlite_stream = os.popen(command)
    output = create_sqlite_stream.read()

    print(output)
