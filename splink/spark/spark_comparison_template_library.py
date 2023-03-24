from ..comparison_template_library import (
    DateComparisonBase,
    NameComparisonBase,
)
from .spark_comparison_library import SparkComparisonProperties


class date_comparison(SparkComparisonProperties, DateComparisonBase):
    pass


class name_comparison(SparkComparisonProperties, NameComparisonBase):
    pass
