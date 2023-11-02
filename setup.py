from setuptools import find_packages, setup

setup(
    name="energy_dagster",
    packages=find_packages(exclude=["energy_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-dbt",
        "dbt-postgres",
        "open-mastr",
        "SQLAlchemy",
        "geopandas",
        "numpy",
        "openpyxl",
        "GeoAlchemy2",
        "docker",
        "cjio",
        "dagster-webserver",
    ],
    extras_require={
        "dev": [
            "pytest",
            "black",
            "dbt-osmosis",
            "sqlfluff",
            "pre-commit",
            "mkdocstrings[python]",
            "mkdocs-material",
        ]
    },
)
