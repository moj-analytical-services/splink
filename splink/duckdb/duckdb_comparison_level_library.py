import warnings

from .comparison_level_library import *  # noqa: F403

warnings.warn(
    "Importing directly from `splink.duckdb.comparison_level_library` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.duckdb.comparison_level_library` going forward.",
    DeprecationWarning,
    stacklevel=2,
)
