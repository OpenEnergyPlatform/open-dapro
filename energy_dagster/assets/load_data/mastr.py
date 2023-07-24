import os

from dagster import asset
from open_mastr import Mastr

from energy_dagster.utils import utils


@asset(key_prefix="raw_mastr", group_name="mastr")
def wind_extended() -> None:
    """Download data on wind turbines from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["wind"])


@asset(key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"wind_extended"})
def biomass_extended() -> None:
    """Download data on biomass power plants from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["biomass"])


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"biomass_extended"}
)
def storage_extended() -> None:
    """Download data on electricity storages from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["storage"])


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"storage_extended"}
)
def storage_units() -> None:
    """Download data on electricity storages from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    pass


@asset(
    key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"storage_extended"}
)
def solar_extended() -> None:
    """Download data on photovoltaic systems from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["solar"])


@asset(key_prefix="raw_mastr", group_name="mastr", non_argument_deps={"solar_extended"})
def market_actors() -> None:
    """Download data on actors of the electricity market from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["market"])
