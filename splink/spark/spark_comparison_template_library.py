from ..comparison_template_library import DateComparisonBase
from .spark_comparison_level_library import jaro_winkler_level, levenshtein_level
from .spark_comparison_library import SparkComparisonProperties


class date_comparison(SparkComparisonProperties, DateComparisonBase):
    @property
    def _levenshtein_level(self):
        return levenshtein_level

    @property
    def _jaro_winkler_level(self):
        return jaro_winkler_level
