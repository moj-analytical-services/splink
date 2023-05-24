# Not all Comparison Template Library functions are currently implemented
# for Athena due to limited string matching capability in
# cll.comparison_level_library

from ..comparison_template_library import (
    PostcodeComparisonBase,
)
from .athena_comparison_library import AthenaComparisonProperties


class postcode_comparison(AthenaComparisonProperties, PostcodeComparisonBase):
    pass
