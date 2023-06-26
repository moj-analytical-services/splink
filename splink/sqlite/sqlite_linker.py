import warnings

from ..exceptions import SplinkDeprecated
from .linker import SQLiteDataFrame, SQLiteLinker  # noqa: F401

warnings.warn(
    "Importing directly from `splink.sqlite.sqlite_linker` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.sqlite.linker` going forward.",
    SplinkDeprecated,
    stacklevel=2,
)
