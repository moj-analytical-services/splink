from ..comparison_template_library import (
    DateComparisonBase,
    ForenameSurnameComparisonBase,
    NameComparisonBase,
    PostcodeComparisonBase,
    EmailComparisonBase
)
from .duckdb_comparison_level_library import distance_function_level
from .duckdb_comparison_library import DuckDBComparisonProperties


class date_comparison(DuckDBComparisonProperties, DateComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class name_comparison(DuckDBComparisonProperties, NameComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class forename_surname_comparison(
    DuckDBComparisonProperties, ForenameSurnameComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class postcode_comparison(DuckDBComparisonProperties, PostcodeComparisonBase):
    pass

class email_comparison(DuckDBComparisonProperties, EmailComparisonBase):
    
    @property
    def _distance_level(self):
        return distance_function_level
