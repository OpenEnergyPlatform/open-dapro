from setuptools import find_packages, setup

setup(
    name="energy_dagster",
    packages=find_packages(exclude=["energy_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-dbt",
        "dagster_shell",
        "dbt-postgres",
        "open-mastr",
        "SQLAlchemy",
        "geopandas",
        "numpy",
        "openpyxl",
        "GeoAlchemy2",
    ],
    extras_require={"dev": ["dagit", "pytest", "black", "dbt-osmosis", "sqlfluff", "pre-commit"]},
)
