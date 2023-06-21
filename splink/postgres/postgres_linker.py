import warnings

from ..exceptions import SplinkDeprecated
from .linker import PostgresDataFrame, PostgresLinker  # noqa: F401

warnings.warn(
    "Importing directly from `splink.postgres.linker` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.postgres.linker` going forward.",
    SplinkDeprecated,
    stacklevel=2,
)
