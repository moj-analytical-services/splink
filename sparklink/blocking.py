import logging

from .logging_utils import log_sql, format_sql
from .sql import comparison_columns_select_expr, sql_gen_comparison_columns
from .settings import _get_columns_to_retain

log = logging.getLogger(__name__)

def sql_gen_and_not_previous_rules(previous_rules: list):
    if previous_rules:
        # Note the isnull function is important here - otherwise
        # you filter out any records with nulls in the previous rules
        # meaning these comparisons get lost
        or_clauses = [f"ifnull(({r}), false)" for r in previous_rules]
        previous_rules = " OR ".join(or_clauses)
        return f"AND NOT ({previous_rules})"
    else:
        return ""

def sql_gen_vertically_concatenate(columns_to_retain: list, table_name_l = "df_l", table_name_r = "df_r"):

    retain = ", ".join(columns_to_retain)

    sql = f"""
    select {retain}, 'left' as source_table
    from {table_name_l}
    union all
    select {retain}, 'right' as source_table
    from {table_name_r}
    """

    return sql

def vertically_concatenate_datasets(df_l, df_r, settings, logger=log, spark=None):

    columns_to_retain = _get_columns_to_retain(settings)
    sql = sql_gen_vertically_concatenate(columns_to_retain)

    df_l.createOrReplaceTempView("df_l")
    df_r.createOrReplaceTempView("df_r")
    log_sql(sql, logger)
    df = spark.sql(sql)
    return df

def sql_gen_block_using_rules(
    link_type: str,
    columns_to_retain: list,
    blocking_rules: list,
    unique_id_col: str = "unique_id",
    table_name_l: str = "df_l",
    table_name_r: str = "df_r",
    table_name_dedupe: str = "df"
):
    """Build a SQL statement that implements a list of blocking rules.

    The left and right tables are aliased as `l` and `r` respectively, so an example
    blocking rule would be `l.surname = r.surname AND l.forename = r.forename`.

    Args:
        link_type: One of 'link_only', 'dedupe_only', or 'link_and_dedupe'
        columns_to_retain: List of columns to keep in returned dataset
        blocking_rules: Each element of the list represents a blocking rule
        unique_id_col (str, optional): The name of the column containing the row's unique_id. Defaults to "unique_id".
        table_name (str, optional): Name of the table. Defaults to "df".

    Returns:
        str: A SQL statement that implements the blocking rules
    """

    # In both these cases the data is in a single table
    # (In the link_and_dedupe case the two tables have already been vertically concatenated)
    if link_type in ['dedupe_only', 'link_and_dedupe']:
        table_name_l = table_name_dedupe
        table_name_r = table_name_dedupe

    if unique_id_col not in columns_to_retain:
        columns_to_retain.insert(0, unique_id_col)

    sql_select_expr = sql_gen_comparison_columns(columns_to_retain)

    if link_type == "link_only":
        where_condition = ""
    else:
        where_condition = f"where l.{unique_id_col} < r.{unique_id_col}"

    sqls = []
    previous_rules =[]
    for rule in blocking_rules:
        not_previous_rules_statement = sql_gen_and_not_previous_rules(previous_rules)
        sql = f"""
        select
        {sql_select_expr}
        from {table_name_l} as l
        inner join {table_name_r} as r
        on
        {rule}
        {not_previous_rules_statement}
        {where_condition}
        """
        previous_rules.append(rule)
        sqls.append(sql)

    sql = "union all".join(sqls)

    return sql


def block_using_rules(
    link_type: str,
    df_l,
    df_r,
    settings,
    columns_to_retain: list=None,
    spark=None,
    unique_id_col="unique_id",
    logger=log
):
    """Apply a series of blocking rules to create a dataframe of record comparisons.
    """
    if columns_to_retain is None:
        columns_to_retain = _get_columns_to_retain(settings)

    if link_type == "dedupe_only":
        df_l.createOrReplaceTempView("df")

    if link_type == "link_only":
        df_l.createOrReplaceTempView("df_l")
        df_r.createOrReplaceTempView("df_r")

    if link_type == "link_and_dedupe":
        df_concat = vertically_concatenate_datasets(df_l, df_r, settings, logger=logger, spark=spark)
        df_concat.createOrReplaceTempView("df")
        df_concat.persist()

    rules = settings["blocking_rules"]

    sql = sql_gen_block_using_rules(link_type, columns_to_retain, rules, unique_id_col)

    log_sql(sql, logger)

    df_comparison = spark.sql(sql)

    if link_type == "link_and_dedupe":
        df_concat.unpersist()

    return df_comparison

def block_using_rules_link_and_dedupe(df_l, df_r, settings: list,  columns_to_retain: list=None,
    spark=None,
    unique_id_col="unique_id",
    logger=log):

    return block_using_rules("link_and_dedupe",
                             df_l,
                             df_r,
                             settings,
                             columns_to_retain,
                             spark,
                             unique_id_col,
                             logger)


def block_using_rules_link_only(df_l, df_r, settings: list,  columns_to_retain: list=None,
    spark=None,
    unique_id_col="unique_id",
    logger=log):

    return block_using_rules("link_only",
                             df_l,
                             df_r,
                             settings,
                             columns_to_retain,
                             spark,
                             unique_id_col,
                             logger)


def block_using_rules_dedupe_only(df, settings: list,  columns_to_retain: list=None,
    spark=None,
    unique_id_col="unique_id",
    logger=log):

    return block_using_rules("dedupe_only",
                             df,
                             None,
                             settings,
                             columns_to_retain,
                             spark,
                             unique_id_col,
                             logger)



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
