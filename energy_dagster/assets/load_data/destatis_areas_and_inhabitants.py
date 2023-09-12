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
def destatis_areas_and_inhabitants() -> pd.DataFrame:
    constants = utils.get_constants("destatis_areas_and_inhabitants")
    raw_data = request.urlopen(constants["url"]).read()
    return pd.read_excel(raw_data, "Onlineprodukt_Gemeinden30062023", skiprows=5)
