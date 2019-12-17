import logging

from .logging_utils import log_sql, format_sql
from .sql import comparison_columns_select_expr, sql_gen_comparison_columns

log = logging.getLogger(__name__)


def sql_gen_block_using_rules(
    columns_to_retain: list,
    blocking_rules: list,
    unique_id_col: str = "unique_id",
    table_name: str = "df",
):
    """Build a SQL statement that implements a list of blocking rules.

    The left and right tables are aliased as `l` and `r` respectively, so an example
    blocking rule would be `l.surname = r.surname AND l.forename = r.forename`.

    Args:
        columns_to_retain: List of columns to keep in returned dataset
        blocking_rules: Each element of the list represents a blocking rule
        unique_id_col (str, optional): The name of the column containing the row's unique_id. Defaults to "unique_id".
        table_name (str, optional): Name of the table. Defaults to "df".

    Returns:
        str: A SQL statement that implements the blocking rules
    """

    if unique_id_col not in columns_to_retain:
        columns_to_retain.insert(0, unique_id_col)

    sql_select_expr = sql_gen_comparison_columns(columns_to_retain)

    sqls = []
    for rule in blocking_rules:
        sql = f"""
        select
        {sql_select_expr}
        from {table_name} as l
        left join {table_name} as r
        on
        {rule}
        where l.{unique_id_col} < r.{unique_id_col}
        """
        sqls.append(sql)

    # Note the 'union' function in pyspark > 2.0 is not the same thing as union in a sql statement
    sql = "union all".join(sqls)

    return sql


def block_using_rules(
    df,
    blocking_rules: list,
    columns_to_retain: list=None,
    spark=None,
    unique_id_col="unique_id",
    logger=log,
):
    """Apply a series of blocking rules to create a dataframe of record comparisons.
    """
    if columns_to_retain is None:
        columns_to_retain = df.columns

    sql = sql_gen_block_using_rules(columns_to_retain, blocking_rules, unique_id_col)

    log_sql(sql, logger)
    df.createOrReplaceTempView("df")
    df_comparison = spark.sql(sql)

    # Think this may be more efficient than using union to join each dataset because we only dropduplicates once
    df_comparison = df_comparison.dropDuplicates()

    return df_comparison


def sql_gen_cartesian_block(
    columns_to_retain: list, unique_id_col: str = "unique_id", table_name: str = "df"
):
    """Build a SQL statement that generates the cartesian product of the input dataset

    Args:
        columns_to_retain: List of columns to keep in returned dataset
        unique_id_col (str, optional): The name of the column containing the row's unique_id. Defaults to "unique_id".
        table_name (str, optional): Name of the table. Defaults to "df".

    Returns:
        str: A SQL statement that will generate the cartesian product
    """

    if unique_id_col not in columns_to_retain:
        columns_to_retain.insert(0, unique_id_col)

    sql_select_expr = sql_gen_comparison_columns(columns_to_retain)

    sql = f"""
    select
    {sql_select_expr}
    from {table_name} as l
    cross join {table_name} as r
    where l.{unique_id_col} < r.{unique_id_col}
    """

    return sql


def cartestian_block(
    df,
    columns_to_retain: list,
    spark=None,
    unique_id_col: str = "unique_id",
    logger=log,
):

    sql = sql_gen_cartesian_block(columns_to_retain, unique_id_col)

    log_sql(sql, log)
    df.createOrReplaceTempView("df")
    df_comparison = spark.sql(sql)

    return df_comparison
