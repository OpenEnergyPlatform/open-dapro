import io
import urllib.request as request
import zipfile

import pandas as pd
from dagster import asset

from energy_dagster.utils import utils


@asset(
    io_manager_key="db_io",
    key_prefix="raw",
    group_name="raw_data",
    compute_kind="python",
)
def zensus() -> pd.DataFrame:
    """Download zensus data from the latest zensus (www.zensus2011.de).
    The link to the dataset can be obtained
    from the `constants.yaml` file in the energy_dagster folder.

    Returns
    -------
    pd.DataFrame
        Zensus data in its raw data format.
    """
    constants = utils.get_constants("zensus")
    zipped_data = request.urlopen(constants["url"]).read()
    with zipfile.ZipFile(io.BytesIO(zipped_data)) as zip_ref:
        with zip_ref.open("Zensus11_Datensatz_Bevoelkerung.csv") as file:
            df = pd.read_csv(file, encoding="latin-1", delimiter=";", low_memory=False)
    return df
