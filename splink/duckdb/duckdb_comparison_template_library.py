from ..comparison_template_library import DateComparisonBase

from .duckdb_comparison_library import DuckDBComparisonProperties
from .duckdb_comparison_level_library import levenshtein_level

class date_comparison(DuckDBComparisonProperties, DateComparisonBase):
    @property
    def _levenshtein_level(self):
        return levenshtein_level