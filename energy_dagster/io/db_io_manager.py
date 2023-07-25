import pandas as pd
from dagster import (
    InitResourceContext,
    InputContext,
    IOManager,
    MetadataValue,
    OutputContext,
    StringSource,
    io_manager,
)
from sqlalchemy import create_engine

# Code from https://github.com/hnawaz007/pythondataanalysis/
# blob/48df26b18a16aeeb8b25bcd0bdd736adb1f7f49f/dagster-project/
# etl/etl/io/db_io_manager.py


class PostgresDataframeIOManager(IOManager):
    def __init__(
        self, uid: str, pwd: str, server: str, db: str, port: str, schema: str
    ) -> None:
        # credentials passed to IO Manager
        self.uid = uid
        self.pwd = pwd
        self.db = db
        self.server = server
        self.port = port
        self.schema = schema

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # Skip handling if the output is None
        if obj is None:
            return

        table_name = context.asset_key[-1][-1]
        context.log.info(table_name)
        context.log.info(context.asset_info)

        #
        engine = create_engine(
            f"postgresql://{self.uid}:{self.pwd}@{self.server}:{self.port}/{self.db}"
        )
        #
        obj.to_sql(
            table_name, engine, schema=self.schema, if_exists="replace", index=False
        )

        context.add_output_metadata(
            {
                "db": self.db,
                "table_name": table_name,
                "descriptive_statistics": MetadataValue.md(
                    obj.describe(include="all").to_markdown()
                ),
            }
        )

    def load_input(self, context: InputContext):
        # upstream_output.asset_key is the asset key given to the Out that we're loading
        table_name = context.upstream_output.asset_key[-1][-1]
        #
        engine = create_engine(
            f"postgresql://{self.uid}:{self.pwd}@{self.server}:{self.port}/{self.db}"
        )
        return pd.read_sql(f"SELECT * FROM {self.schema}.{table_name}", engine)


@io_manager(
    config_schema={
        "uid": StringSource,
        "pwd": StringSource,
        "server": StringSource,
        "db": StringSource,
        "port": StringSource,
        "schema": StringSource,
    }
)
def postgres_pandas_io_manager(
    init_context: InitResourceContext,
) -> PostgresDataframeIOManager:
    return PostgresDataframeIOManager(
        pwd=init_context.resource_config["pwd"],
        uid=init_context.resource_config["uid"],
        server=init_context.resource_config["server"],
        db=init_context.resource_config["db"],
        port=init_context.resource_config["port"],
        schema=init_context.resource_config["schema"],
    )
