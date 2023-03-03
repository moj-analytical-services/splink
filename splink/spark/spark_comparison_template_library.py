from ..comparison_template_library import DateComparisonBase

from .spark_comparison_library import SparkComparisonProperties
from .spark_comparison_level_library import levenshtein_level

class date_comparison(SparkComparisonProperties, DateComparisonBase):
    @property
    def _levenshtein_level(self):
        return levenshtein_level