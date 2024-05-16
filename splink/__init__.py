from typing import TYPE_CHECKING

# Explicitly declare exported names to avoid 'imported but unused' linting issues
__all__ = [
    "block_on",
    "splink_datasets",
    "Linker",
    "SettingsCreator",
    "SQLiteAPI",
    "SparkAPI",
    "DuckDBAPI",
    "PostgresAPI",
]


from splink.blocking_rule_library import block_on
from splink.datasets import splink_datasets
from splink.linker import Linker
from splink.settings_creator import SettingsCreator
from splink.sqlite.database_api import SQLiteAPI

# The following is a workaround for the fact that dependencies of postgres, spark
# and duckdb may not be installed, but we don't want this to prevent import
# of the other backends.

# This enables auto-complete to be used to import the various DBAPIs
# and ensures that typing information is retained so e.g. the arguments autocomplete
# without importing them at runtime
if TYPE_CHECKING:
    from splink.duckdb.database_api import DuckDBAPI
    from splink.postgres.database_api import PostgresAPI
    from splink.spark.database_api import SparkAPI


# Use getarr to make the error appear at the point of use
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
                "installed. Please `pip install` the required package(s) as "
                "specified in the optional dependencies in pyproject.toml"
            ) from err
    raise AttributeError(f"module 'splink' has no attribute '{name}'") from None


__version__ = "4.0.0.dev4"
