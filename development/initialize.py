import os
import subprocess
import time

import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import DDL, create_engine, exc


def get_engine() -> sqlalchemy.engine.Engine:
    try:
        host = os.environ["server"]
    except KeyError:
        load_dotenv()
        host = os.environ["server"]
    port = os.environ["port"]
    user = os.environ["uid"]
    password = os.environ["pwd"]
    dbname = os.environ["db"]
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )


def setup_docker() -> None:
    """Initialize a PostgreSQL database with docker compose"""
    conf_file_path = os.path.abspath(os.path.dirname(__file__))
    subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=conf_file_path,
    )


def test_connection(engine: sqlalchemy.engine.Engine) -> bool:
    """Test connection to the specified database

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Engine of the database

    Returns
    -------
    Whether the connection was successful or not
    """
    counter_connection_test = 0
    number_total_retries = 5
    time.sleep(5)
    while counter_connection_test < number_total_retries:
        try:
            connection = engine.connect()
            connection.close()
            print("Dockerized database was started and can be accessed.")
            return True
        except exc.DBAPIError:
            time.sleep(3)
            counter_connection_test += 1
            print(
                "Connection to database could not be established."
                f" Retries {counter_connection_test}/{number_total_retries}"
            )
    return False


def create_schemas(schema_names: list, engine: sqlalchemy.engine.Engine) -> None:
    """Create schemas in the specified database

    Parameters
    ----------
    schema_names : list
        List of schema names to be created
    engine : sqlalchemy.engine.Engine
        Engine of the database
    """
    for schema_name in schema_names:
        create_schema_stmt = DDL(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        with engine.begin() as connection:
            connection.execute(create_schema_stmt)
        print(f"Schema {schema_name} was created.")


def initialize_development_environment() -> None:
    initialize_database()
    initialize_dagster_home()
    install_dbt_packages()


def install_dbt_packages() -> None:
    current_dir = os.getcwd()
    dbt_dir = os.path.join(current_dir, "dbt")
    subprocess.run(["dbt", "deps"], cwd=dbt_dir)
    print("DBT packages were installed.")


def initialize_dagster_home() -> None:
    try:
        DAGSTER_HOME = os.environ["DAGSTER_HOME"]
    except KeyError:
        load_dotenv()
        DAGSTER_HOME = os.environ["DAGSTER_HOME"]
    dagster_home_abspath = os.path.abspath(os.path.expanduser(DAGSTER_HOME))
    if not os.path.exists(dagster_home_abspath):
        os.makedirs(dagster_home_abspath)
        print(f"Dagster home was created at {dagster_home_abspath}")
    else:
        print(f"Dagster home already exists at {dagster_home_abspath}")


def initialize_database() -> None:
    engine = get_engine()
    setup_docker()
    if test_connection(engine):
        create_schemas(["raw"], engine)
        print("Database was initialized successfully.")


if __name__ == "__main__":
    initialize_development_environment()
