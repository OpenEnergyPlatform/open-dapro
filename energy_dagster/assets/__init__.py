import os

from dagster import file_relative_path, load_assets_from_modules
from dagster_dbt import load_assets_from_dbt_project
from dotenv import load_dotenv

from energy_dagster.assets.load_data import (
    alkis_area_usage,
    buildings_lod2,
    charging_points,
    geo_boundaries,
    mastr,
    osm,
    zensus,
)

try:
    DBT_PROFILES_PATH = file_relative_path(
        __file__, f"../../dbt/config/{os.environ['DBT_PROFILE_FOLDER']}"
    )
except KeyError:
    load_dotenv()
    DBT_PROFILES_PATH = file_relative_path(
        __file__, f"../../dbt/config/{os.environ['DBT_PROFILE_FOLDER']}"
    )

DBT_PROJECT_PATH = file_relative_path(__file__, "../../dbt")

all_assets = load_assets_from_modules(
    [
        charging_points,
        zensus,
        mastr,
        geo_boundaries,
        buildings_lod2,
        osm,
        alkis_area_usage,
    ]
)
dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES_PATH,
    key_prefix=["energy_dbt"],
)
