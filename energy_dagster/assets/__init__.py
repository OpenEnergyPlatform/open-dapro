from dagster import load_assets_from_modules, file_relative_path
from dagster_dbt import load_assets_from_dbt_project
from dotenv import load_dotenv
import os

from energy_dagster.assets.load_data import (
    charging_points,
    mastr,
    zensus,
    geo_boundaries,
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

all_assets = load_assets_from_modules([charging_points, zensus, mastr, geo_boundaries])
dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES_PATH,
    key_prefix=["energy_dbt"],
)
