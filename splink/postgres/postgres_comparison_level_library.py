from ..comparison_level_composition import and_, not_, or_  # noqa: F401
from ..comparison_level_library import (
    ArrayIntersectLevelBase,
    ColumnsReversedLevelBase,
    DateDiffLevelBase,
    DistanceFunctionLevelBase,
    DistanceInKMLevelBase,
    ElseLevelBase,
    ExactMatchLevelBase,
    JaccardLevelBase,
    JaroWinklerLevelBase,
    LevenshteinLevelBase,
    NullLevelBase,
    PercentageDifferenceLevelBase,
)
from .postgres_base import PostgresBase


class null_level(PostgresBase, NullLevelBase):
    pass


class exact_match_level(PostgresBase, ExactMatchLevelBase):
    pass


class else_level(PostgresBase, ElseLevelBase):
    pass


class columns_reversed_level(PostgresBase, ColumnsReversedLevelBase):
    pass


class distance_function_level(PostgresBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(PostgresBase, LevenshteinLevelBase):
    pass


class array_intersect_level(PostgresBase, ArrayIntersectLevelBase):
    pass


class percentage_difference_level(PostgresBase, PercentageDifferenceLevelBase):
    pass


class distance_in_km_level(PostgresBase, DistanceInKMLevelBase):
    pass


class datediff_level(PostgresBase, DateDiffLevelBase):
    pass
