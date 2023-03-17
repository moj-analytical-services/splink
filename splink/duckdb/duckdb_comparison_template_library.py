from ..comparison_template_library import (
    DateComparisonBase,
    NameComparisonBase,
    FullNameComparisonBase,
)
from .duckdb_comparison_library import DuckDBComparisonProperties


class date_comparison(DuckDBComparisonProperties, DateComparisonBase):
    pass


class name_comparison(DuckDBComparisonProperties, NameComparisonBase):
    pass


class fullname_comparison(DuckDBComparisonProperties, FullNameComparisonBase):
    pass
