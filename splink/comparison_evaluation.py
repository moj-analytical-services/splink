import re

import logging
from .params import Params

from .maximisation_step import run_maximisation_step

logger = logging.getLogger(__name__)
from .logging_utils import _format_sql


def get_largest_blocks(blocking_rule, df, spark, limit=5):
    """
    For a given blocking rule, find out which will be the largest blocks
    """

    parts = re.split(" |=", blocking_rule)
    parts = [p for p in parts if "l." in p]
    parts = [p.replace("l.", "") for p in parts]

    col_expr = ", ".join(parts)

    filter_nulls_expr = " and ".join(f"{p} is not null" for p in parts)

    sql = f"""
    SELECT {col_expr}, count(*) as count
    FROM df
    WHERE {filter_nulls_expr}
    GROUP BY {col_expr}
    ORDER BY count(*) desc
    LIMIT {limit}
    """
    df.createOrReplaceTempView("df")
    return spark.sql(sql)

