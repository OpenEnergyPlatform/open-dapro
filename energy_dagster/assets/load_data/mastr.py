import os

from dagster import asset
from open_mastr import Mastr

from energy_dagster.utils import utils


@asset(key_prefix="raw_mastr", group_name="mastr", compute_kind="python")
def download_mastr() -> None:
    """Download data on biomass power plants from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["nuclear"])


@asset(
    key_prefix="raw_mastr",
    group_name="mastr",
    non_argument_deps={"download_mastr"},
    compute_kind="python",
)
def wind_extended() -> None:
    """Download data on wind turbines from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["wind"])


@asset(
    key_prefix="raw_mastr",
    group_name="mastr",
    non_argument_deps={"download_mastr"},
    compute_kind="python",
)
def biomass_extended() -> None:
    """Download data on biomass power plants from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["biomass"])


@asset(
    key_prefix="raw_mastr",
    group_name="mastr",
    non_argument_deps={"download_mastr"},
    compute_kind="python",
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
    key_prefix="raw_mastr",
    group_name="mastr",
    non_argument_deps={"download_mastr"},
    compute_kind="python",
)
def solar_extended() -> None:
    """Download data on photovoltaic systems from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["solar"])


@asset(
    key_prefix="raw_mastr",
    group_name="mastr",
    non_argument_deps={"download_mastr"},
    compute_kind="python",
)
def market_actors() -> None:
    """Download data on actors of the electricity market from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["market"])


@asset(
    key_prefix="raw_mastr",
    group_name="mastr",
    non_argument_deps={"download_mastr"},
    compute_kind="python",
)
def combustion_extended() -> None:
    """Download data on combustion power plants from the german public
    registry 'Marktstammdatenregister (MaStR)' using the python package
    open-mastr.
    """
    engine = utils.get_engine()
    db = Mastr(engine=engine)
    db.download(date=os.environ["MASTR_DOWNLOAD_DATE"], data=["combustion"])
