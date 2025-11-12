from sys import version_info
from typing import TYPE_CHECKING
from warnings import warn

from splink.internals.blocking_rule_library import block_on
from splink.internals.column_expression import ColumnExpression
from splink.internals.datasets import splink_datasets
from splink.internals.linker import Linker
from splink.internals.settings_creator import SettingsCreator

# The following is a workaround for the fact that dependencies of particular backends
# may not be installed, but we don't want this to prevent import
# of the other backends.

# This enables auto-complete to be used to import the various DBAPIs
# and ensures that typing information is retained so e.g. the arguments autocomplete
# without importing them at runtime
if TYPE_CHECKING:
    from splink.internals.duckdb.database_api import DuckDBAPI
    from splink.internals.spark.database_api import SparkAPI

_LOWEST_SUPPORTED_MINOR_VERSION = 10

if (installed_minor_version := version_info.minor) < _LOWEST_SUPPORTED_MINOR_VERSION:
    warn(
        (
            f"Python 3.{installed_minor_version} has reached end-of-life.  "
            "Future releases of Splink may no longer be compatible with "
            "this python version.\n"
            "Please consider upgrading your python version if you wish "
            "to continue to be able to install the latest version of Splink."
        ),
        category=DeprecationWarning,
        stacklevel=2,
    )


# Use getarr to make the error appear at the point of use
def __getattr__(name):
    try:
        if name == "SparkAPI":
            from splink.internals.spark.database_api import SparkAPI

            return SparkAPI
        elif name == "DuckDBAPI":
            from splink.internals.duckdb.database_api import DuckDBAPI

            return DuckDBAPI
    except ImportError as err:
        if name in ["SparkAPI", "DuckDBAPI"]:
            raise ImportError(
                f"{name} cannot be imported because its dependencies are not "
                "installed. Please `pip install` the required package(s) as "
                "specified in the optional dependencies in pyproject.toml"
            ) from err
    raise AttributeError(f"module 'splink' has no attribute '{name}'") from None


__version__ = "4.0.11"


__all__ = [
    "block_on",
    "ColumnExpression",
    "DuckDBAPI",
    "Linker",
    "SettingsCreator",
    "SparkAPI",
    "splink_datasets",
]
