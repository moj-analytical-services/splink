from typing import TYPE_CHECKING

from splink.internals.blocking_rule_library import block_on
from splink.internals.column_expression import ColumnExpression
from splink.internals.datasets import splink_datasets
from splink.internals.linker import Linker
from splink.internals.settings_creator import SettingsCreator

# The following is a workaround for the fact that dependencies of postgres, spark
# and duckdb may not be installed, but we don't want this to prevent import
# of the other backends.

# This enables auto-complete to be used to import the various DBAPIs
# and ensures that typing information is retained so e.g. the arguments autocomplete
# without importing them at runtime
if TYPE_CHECKING:
    from splink.internals.duckdb.database_api import DuckDBAPI
    from splink.internals.postgres.database_api import PostgresAPI
    from splink.internals.spark.database_api import SparkAPI


# Use getarr to make the error appear at the point of use
def __getattr__(name):
    try:
        if name == "SparkAPI":
            from splink.internals.spark.database_api import SparkAPI

            return SparkAPI
        elif name == "DuckDBAPI":
            from splink.internals.duckdb.database_api import DuckDBAPI

            return DuckDBAPI
        elif name == "PostgresAPI":
            from splink.internals.postgres.database_api import PostgresAPI

            return PostgresAPI
    except ImportError as err:
        if name in ["SparkAPI", "DuckDBAPI", "PostgresAPI"]:
            raise ImportError(
                f"{name} cannot be imported because its dependencies are not "
                "installed. Please `pip install` the required package(s) as "
                "specified in the optional dependencies in pyproject.toml"
            ) from err
    raise AttributeError(f"module 'splink' has no attribute '{name}'") from None


__version__ = "4.0.0.dev8"


__all__ = [
    "block_on",
    "ColumnExpression",
    "DuckDBAPI",
    "Linker",
    "PostgresAPI",
    "SettingsCreator",
    "SparkAPI",
    "splink_datasets",
    "SQLiteAPI",
]
