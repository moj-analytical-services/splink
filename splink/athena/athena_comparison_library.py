from ..comparison_level_library import (
    _mutable_params,
)


from ..comparison_library import (  # noqa: F401
    exact_match,
    levenshtein_at_thresholds,
)

_mutable_params["dialect"] = "presto"
_mutable_params["levenshtein"] = "levenshtein_distance"
