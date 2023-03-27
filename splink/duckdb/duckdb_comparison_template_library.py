from ..comparison_template_library import DateComparisonBase, NameComparisonBase, FirstnameSurnameComparisonBase
from .duckdb_comparison_library import DuckDBComparisonProperties


class date_comparison(DuckDBComparisonProperties, DateComparisonBase):
    pass


class name_comparison(DuckDBComparisonProperties, NameComparisonBase):
    pass

class firstname_surname_comparison(DuckDBComparisonProperties, FirstnameSurnameComparisonBase):
    pass