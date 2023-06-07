import warnings

from ..exceptions import SplinkDeprecated
from .linker import AthenaDataFrame, AthenaLinker  # noqa: F401

warnings.warn(
    "Importing directly from `splink.athena.athena_linker` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.athena.linker` going forward.",
    SplinkDeprecated,
    stacklevel=2,
)
