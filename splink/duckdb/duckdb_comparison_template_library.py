from ..comparison_template_library import DateComparisonBase, NameComparisonBase
from .duckdb_comparison_library import DuckDBComparisonProperties
from .duckdb_comparison_level_library import distance_function_level


class date_comparison(DuckDBComparisonProperties, DateComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class name_comparison(DuckDBComparisonProperties, NameComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level
