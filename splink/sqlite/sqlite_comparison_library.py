from ..comparison_level_library import (
    _mutable_params,
)


from ..comparison_library import (  # noqa: F401
    exact_match,
    distance_function_at_thresholds,
)

_mutable_params["dialect"] = "sqlite"
