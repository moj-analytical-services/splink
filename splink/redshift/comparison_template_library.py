# Not all Comparison Template Library functions are currently implemented
# for Redshift due to limited string matching capability in
# cll.comparison_level_library

import logging

logger = logging.getLogger(__name__)

logger.warning(
    "The Comparison Template Library is not currently implemented "
    "for Redshift due to limited string matching capability in "
    "`cll.comparison_level_library`"
)
