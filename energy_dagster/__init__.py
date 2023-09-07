import os

from dagster import Definitions, FilesystemIOManager, load_assets_from_modules
from dagster_dbt import DbtCliClientResource

from energy_dagster import assets
from energy_dagster.assets import DBT_PROFILES_PATH, DBT_PROJECT_PATH
from energy_dagster.io import db_io_manager, postgis_io_manager
from energy_dagster.jobs import mastr_schedule, raw_data_schedule

BASE_DIR = os.path.join(os.path.expanduser("~"), ".dagster", "dagster_energy_files")


resources = {
    "db_io": db_io_manager.postgres_pandas_io_manager.configured(
        {
            "server": {"env": "server"},
            "db": {"env": "db"},
            "uid": {"env": "uid"},
            "pwd": {"env": "pwd"},
            "port": {"env": "port"},
            "schema": {"env": "schema"},
        }
    ),
    "dbt_marts_to_geopandas": postgis_io_manager.postgis_geopandas_io_manager.configured(
        {
            "server": {"env": "server"},
            "db": {"env": "db"},
            "uid": {"env": "uid"},
            "pwd": {"env": "pwd"},
            "port": {"env": "port"},
            "schema": "data_marts",
        }
    ),
    "postgis_io": postgis_io_manager.postgis_geopandas_io_manager.configured(
        {
            "server": {"env": "server"},
            "db": {"env": "db"},
            "uid": {"env": "uid"},
            "pwd": {"env": "pwd"},
            "port": {"env": "port"},
            "schema": {"env": "schema"},
        }
    ),
    "file_io": FilesystemIOManager(base_dir=BASE_DIR),
    "dbt": DbtCliClientResource(
        project_dir=DBT_PROJECT_PATH,
        profiles_dir=DBT_PROFILES_PATH,
    ),
}


defs = Definitions(
    assets=load_assets_from_modules([assets]),
    schedules=[mastr_schedule, raw_data_schedule],
    resources=resources,
)
