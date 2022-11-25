from ..comparison_level_library import (
    _mutable_params,
)

from ..comparison_library import (  # noqa: F401
    exact_match,
    DistanceFunctionAtThresholdsComparisonBase,
)
from .sqlite_base import (
    SqliteBase,
)
from .sqlite_comparison_level_library import (
    distance_function_level,
)

_mutable_params["dialect"] = "sqlite"


class distance_function_at_thresholds(
    SqliteBase, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level
