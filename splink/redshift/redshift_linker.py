import warnings

from ..exceptions import SplinkDeprecated
from .linker import RedshiftDataFrame, RedshiftLinker  # noqa: F401

warnings.warn(
    "Importing directly from `splink.redshift.redshift_linker` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.redshift.linker` going forward.",
    SplinkDeprecated,
    stacklevel=2,
)
