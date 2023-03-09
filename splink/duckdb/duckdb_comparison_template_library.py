from ..comparison_template_library import DateComparisonBase
from .duckdb_comparison_level_library import jaro_winkler_level, levenshtein_level
from .duckdb_comparison_library import DuckDBComparisonProperties


class date_comparison(DuckDBComparisonProperties, DateComparisonBase):
    @property
    def _levenshtein_level(self):
        return levenshtein_level

    @property
    def _jaro_winkler_level(self):
        return jaro_winkler_level
