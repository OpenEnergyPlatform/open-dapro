from dagster import asset

from open_mastr import Mastr
import os
from energy_dagster.utils import utils


@asset(key_prefix="raw_mastr", group_name="mastr")
def wind_extended(context) -> None:
    try:
        engine = utils.get_engine()
        db = Mastr(engine=engine)
        db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["wind"])

    except Exception as e:
        context.log.info(str(e))


@asset(key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"wind_extended"})
def biomass_extended(context) -> None:
    try:
        engine = utils.get_engine()
        db = Mastr(engine=engine)
        db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["biomass"])

    except Exception as e:
        context.log.info(str(e))


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"biomass_extended"}
)
def storage_extended(context) -> None:
    try:
        engine = utils.get_engine()
        db = Mastr(engine=engine)
        db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["storage"])
    except Exception as e:
        context.log.info(str(e))


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"storage_extended"}
)
def storage_units() -> None:
    pass


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"storage_extended"}
)
def solar_extended(context) -> None:
    try:
        engine = utils.get_engine()
        db = Mastr(engine=engine)
        db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["solar"])

    except Exception as e:
        context.log.info(str(e))


@asset(key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"solar_extended"})
def market_actors(context) -> None:
    try:
        engine = utils.get_engine()
        db = Mastr(engine=engine)
        db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["market"])

    except Exception as e:
        context.log.info(str(e))
