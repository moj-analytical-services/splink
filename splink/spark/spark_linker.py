import warnings

from ..exceptions import SplinkDeprecated
from .linker import SparkDataFrame, SparkLinker  # noqa: F401

warnings.warn(
    "Importing directly from `splink.spark.spark_linker` "
    "is deprecated and will be removed in Splink v4. "
    "Please import from `splink.spark.linker` going forward.",
    SplinkDeprecated,
    stacklevel=2,
)
