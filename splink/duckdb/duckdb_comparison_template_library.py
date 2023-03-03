from .duckdb_comparison_library import DuckDBComparisonProperties
from ..comparison_template_library import DateComparisonBase


class date_comparison(DuckDBComparisonProperties, DateComparisonBase):
    pass
