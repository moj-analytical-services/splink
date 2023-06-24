# The Comparison Template Library is not currently implemented
# for Postgres due to limited string matching capability in
# cll.comparison_level_library

import logging

logger = logging.getLogger(__name__)

logger.warn(
    "The Comparison Template Library is not currently implemented "
    "for Postgres due to limited string matching capability in "
    "`cll.comparison_level_library`"
)
