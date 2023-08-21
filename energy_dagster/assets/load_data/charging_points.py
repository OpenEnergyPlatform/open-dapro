import urllib.request as request

import pandas as pd
from dagster import asset

from energy_dagster.utils import utils


@asset(
    io_manager_key="db_io",
    key_prefix="raw",
    group_name="raw_data",
    compute_kind="python",
)
def charging_points() -> pd.DataFrame:
    """Download charging points data from Ladesaeulenregister offered by
    the german 'bundesnetzagentur'. The link to the dataset can be obtained
    from the `constants.yaml` file in the energy_dagster folder.

    Returns
    -------
    pd.DataFrame
        Charging points in its raw data format.
    """
    constants = utils.get_constants("ladesaeulenregister")
    raw_data = request.urlopen(constants["url"]).read()
    return pd.read_excel(raw_data, skiprows=10)
