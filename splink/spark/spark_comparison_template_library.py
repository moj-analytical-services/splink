from ..comparison_template_library import (
    DateComparisonBase,
    NameComparisonBase,
    FullNameComparisonBase,
)
from .spark_comparison_library import SparkComparisonProperties


class date_comparison(SparkComparisonProperties, DateComparisonBase):
    pass


class name_comparison(DuckDBComparisonProperties, NameComparisonBase):
    pass


class fullname_comparison(DuckDBComparisonProperties, FullNameComparisonBase):
    pass
