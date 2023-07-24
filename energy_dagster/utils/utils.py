import os
import urllib.request as request
from pathlib import Path

import yaml
from sqlalchemy import create_engine


def get_engine_settings():
    host = os.environ["server"]
    port = os.environ["port"]
    user = os.environ["uid"]
    password = os.environ["pwd"]
    dbname = os.environ["db"]

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "dbname": dbname,
    }


def get_dagster_home() -> str:
    """Returns the absolute path to the dagster home directory.

    Returns
    -------
    str
        Path to the dagster home directory
    """
    dagster_home = os.environ.get("DAGSTER_HOME", "~/.dagster")
    return os.path.abspath(os.path.expanduser(dagster_home))


def get_dagster_data_path() -> str:
    """Returns the absolute path to the directory where data from pipelines
    is saved.

    Returns
    -------
    str
        Path to the dagster data directory
    """
    return os.path.join(get_dagster_home(), "data")


def get_engine(schema="public"):
    engine_settings = get_engine_settings()

    host = engine_settings["host"]
    port = engine_settings["port"]
    user = engine_settings["user"]
    password = engine_settings["password"]
    dbname = engine_settings["dbname"]

    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}",
        connect_args={"options": f"-csearch_path={schema}"},
    )


def get_constants(data_source: str):
    """Downloads a file specified in constants.yaml

    Parameters
    ----------
    data_source : str
        data source from cosntants.yaml.

    Returns
    -------
    constants
        constants from data_source
    """
    current_folder = Path(__file__).resolve().parent

    with open(current_folder / "constants.yaml", "r") as f:
        constants = yaml.safe_load(f)

    url = constants["data_sources"][data_source]["url"]
    filename = constants["data_sources"][data_source]["filename"]

    return {
        "url": url,
        "filename": filename,
        "zipfile_path": constants["data_sources"][data_source].get("zipfile_path"),
        "csv_filename": constants["data_sources"][data_source].get("csv_filename"),
    }


def download_from_constants(data_source: str):
    """Downloads a file specified in constants.yaml

    Parameters
    ----------
    data_source : str
        data source from cosntants.yaml.

    Returns
    -------
    constants & download_path
        constants from data_source and
        folder path where the file is saved to.
    """
    current_folder = Path(__file__).resolve().parent

    with open(current_folder / "constants.yaml", "r") as f:
        constants = yaml.safe_load(f)

    url = constants["data_sources"][data_source]["url"]
    save_directory = constants["data_sources"][data_source]["save_directory"]
    filename = constants["data_sources"][data_source]["filename"]

    save_directory_path = os.path.join(get_dagster_data_path(), save_directory)
    download_from_url(url=url, save_directory=save_directory_path, filename=filename)

    return {
        "url": url,
        "save_directory": save_directory,
        "save_directory_path": save_directory_path,
        "filename": filename,
        "zipfile_path": constants["data_sources"][data_source].get("zipfile_path"),
        "csv_filename": constants["data_sources"][data_source].get("csv_filename"),
        "download_path": os.path.join(save_directory_path, filename),
    }


def download_from_url(url: str, save_directory: str, filename: str) -> None:
    """Downloads a file from a given url and saves it to the given path

    Parameters
    ----------
    url : str
        url to download from.
    save_directory : str
        folder path where the file is saved.
    filename : str
        name of the file.
    """
    if not os.path.exists(save_directory):
        os.makedirs(save_directory)

    save_path = os.path.join(save_directory, filename)

    if os.path.isfile(save_path):
        print(f"File {filename} already downloaded.")
        return None
    with open(save_path, "wb") as f:
        f.write(request.urlopen(url).read())
