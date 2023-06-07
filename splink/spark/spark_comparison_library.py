import warnings

from .comparison_library import *  # noqa: F403

warnings.warn(
    "Importing directly from `splink.spark.comparison_library` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.spark.comparison_library` going forward.",
    DeprecationWarning,
    stacklevel=2,
)
