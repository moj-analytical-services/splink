import warnings

from ..exceptions import SplinkDeprecated
from .comparison_library import *  # noqa: F403

warnings.warn(
    "Importing directly from `splink.spark.spark_comparison_library` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.spark.comparison_library` going forward.",
    SplinkDeprecated,
    stacklevel=2,
)
