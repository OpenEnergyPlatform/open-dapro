from dagster import asset

import pandas as pd
from energy_dagster.utils import utils
import urllib.request as request


@asset(io_manager_key="db_io", key_prefix="raw", group_name="raw_data")
def charging_points() -> pd.DataFrame:
    constants = utils.get_constants("ladesaeulenregister")
    raw_data = request.urlopen(constants["url"]).read()
    df = pd.read_excel(raw_data, skiprows=10)
    return df
