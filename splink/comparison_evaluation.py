import re

import logging
from .params import Params

from .maximisation_step import run_maximisation_step

logger = logging.getLogger(__name__)
from .logging_utils import format_sql

def view_matches(df_e, df_comparison, spark):
    df_e.createOrReplaceTempView('df_e')
    df_comparison.createOrReplaceTempView('df_comparison')

    if "tf_adjusted_match_prob" in df_e.columns:
        adj_col = "e.tf_adjusted_match_prob, "
    else:
        adj_col = ""

    sql = f"""
    select e.match_probability, {adj_col} c.*
    from df_e as e
    left join df_comparison as c
    on e.unique_id_l = c.unique_id_l
    and
    e.unique_id_r = c.unique_id_r

    """

    return spark.sql(sql)


def get_real_params(df_comparison, df_with_gamma, spark, est_params):

    gamma_cols = [c for c in df_with_gamma.columns if re.match(r"^gamma_\d$", c)]
    df_with_gamma.createOrReplaceTempView('df_with_gamma')
    df_comparison.createOrReplaceTempView('df_comparison')

    # Want match probability, gammas, label
    gamma_select_expr = ", ".join([f"g.{c}" for c in gamma_cols])

    # This dataset looks like df_e, but instead of match probability it's got a 1,0 label

    sql = f"""
    select {gamma_select_expr},
    cast(c.group_l == c.group_r as int) as match_probability
    from df_with_gamma as g
    left join df_comparison as c
    on g.unique_id_l = c.unique_id_l
    and
    g.unique_id_r = c.unique_id_r

    """
    gamma_e_label = spark.sql(sql)

    p = Params(est_params.gamma_settings)
    run_maximisation_step(gamma_e_label, spark, p)
    return p.params


def comparison_with_match_prob(df_comparison, df_e, spark):
    df_e.createOrReplaceTempView('df_e')
    df_comparison.createOrReplaceTempView('df_comparison')

    gamma_cols = [f"e.{c}" for c in df_e.columns if re.match(r"^gamma_\d$", c)]

    gamma_expr = ", ".join(gamma_cols)

    sql = f"""
    select e.match_probability, {gamma_expr}, c.*
    from df_e as e
    left join df_comparison as c
    on e.unique_id_l = c.unique_id_l
    and
    e.unique_id_r = c.unique_id_r

    """
    return spark.sql(sql)


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

