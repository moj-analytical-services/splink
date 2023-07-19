import warnings

from ..exceptions import SplinkDeprecated
from .linker import DuckDBDataFrame, DuckDBLinker  # noqa: F401

warnings.warn(
    "Importing directly from `splink.duckdb.duckdb_linker` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.duckdb.linker` going forward.",
    SplinkDeprecated,
    stacklevel=2,
)
