from ..comparison_template_library import (
    DateComparisonBase,
    ForenameSurnameComparisonBase,
    NameComparisonBase,
    PostcodeComparisonBase,
)
from .spark_comparison_level_library import distance_function_level
from .spark_comparison_library import SparkComparisonProperties


class date_comparison(SparkComparisonProperties, DateComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class name_comparison(SparkComparisonProperties, NameComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class forename_surname_comparison(
    SparkComparisonProperties, ForenameSurnameComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class postcode_comparison(SparkComparisonProperties, PostcodeComparisonBase):
    pass
