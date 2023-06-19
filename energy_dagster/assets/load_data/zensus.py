from dagster import asset

import pandas as pd
from energy_dagster.utils import utils
import urllib.request as request
import io
import zipfile


@asset(io_manager_key="db_io", key_prefix="raw", group_name="raw_data")
def zensus(context) -> pd.DataFrame:
    try:
        constants = utils.get_constants("zensus")
        zipped_data = request.urlopen(constants["url"]).read()
        with zipfile.ZipFile(io.BytesIO(zipped_data)) as zip_ref:
            with zip_ref.open("Zensus11_Datensatz_Bevoelkerung.csv") as file:
                df = pd.read_csv(
                    file, encoding="latin-1", delimiter=";", low_memory=False
                )
        return df
    except Exception as e:
        context.log.info(str(e))
