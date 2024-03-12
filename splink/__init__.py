from splink.blocking_rule_library import block_on
from splink.datasets import splink_datasets
from splink.linker import Linker
from splink.settings_creator import SettingsCreator
from splink.sqlite.database_api import SQLiteAPI

# try except in case backends are not installed e.g. pyspark


def __getattr__(name):
    try:
        if name == "SparkAPI":
            from splink.spark.database_api import SparkAPI

            return SparkAPI
        elif name == "DuckDBAPI":
            from splink.duckdb.database_api import DuckDBAPI

            return DuckDBAPI
        elif name == "PostgresAPI":
            from splink.postgres.database_api import PostgresAPI

            return PostgresAPI
    except ImportError as err:
        if name in ["SparkAPI", "DuckDBAPI", "PostgresAPI"]:
            raise ImportError(
                f"{name} cannot be imported because its dependencies are not "
                "installed. Please `pip install` the required package."
            ) from err
    raise AttributeError(f"module 'splink' has no attribute '{name}'") from None


__version__ = "4.0.0.dev2"
