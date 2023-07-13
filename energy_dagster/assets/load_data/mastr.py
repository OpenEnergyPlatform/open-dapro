from dagster import asset

from open_mastr import Mastr
import os
from energy_dagster.utils import utils


@asset(key_prefix="raw_mastr", group_name="mastr")
def wind_extended() -> None:
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["wind"])


@asset(key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"wind_extended"})
def biomass_extended() -> None:
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["biomass"])


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"biomass_extended"}
)
def storage_extended() -> None:
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["storage"])


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"storage_extended"}
)
def storage_units() -> None:
    pass


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"storage_extended"}
)
def solar_extended() -> None:
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["solar"])


@asset(key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"solar_extended"})
def market_actors() -> None:
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["market"])
