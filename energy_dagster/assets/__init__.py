from dagster import load_assets_from_modules
from dagster_dbt import load_assets_from_dbt_project

import os

from energy_dagster.assets.load_data import (
    charging_points,
    mastr,
    zensus,
    geo_boundaries,
)

DBT_PROJECT_PATH = os.environ["DBT_PROJECT_PATH"]
DBT_PROFILES_PATH = os.environ["DBT_PROFILES_PATH"]

all_assets = load_assets_from_modules([charging_points, zensus, mastr, geo_boundaries])
dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES_PATH,
    key_prefix=["energy_dbt"],
)
