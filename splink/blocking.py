import logging
from .ordered_set import OrderedSet
from .settings import ComparisonColumn

from typeguard import typechecked

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from .logging_utils import _format_sql

logger = logging.getLogger(__name__)


def sql_gen_comparison_columns(columns: list) -> str:
    """Build SQL expression that renames columns and sets them aside each other for comparisons

     Args:
         columns (list): List of cols

    Examples:
         >>> sql_gen_comparison_columns(["name", "dob"])
         "name_l, name_r, dob_l, dob_r"

     Returns:
         SQL expression
    """

    l = [f"l.{c} as {c}_l" for c in columns]
    r = [f"r.{c} as {c}_r" for c in columns]
    both = zip(l, r)
    flat_list = [item for sublist in both for item in sublist]
    return ", ".join(flat_list)


def _retain_source_dataset_column(settings_dict, df):
    # Want to retain source dataset column in all cases
    # except when link type is dedupe only
    # and the column does not exist in the data
    if settings_dict["link_type"] != "dedupe_only":
        return True

    source_dataset_colname = settings_dict.get(
        "source_dataset_column_name", "source_dataset"
    )
    if source_dataset_colname in df.columns:
        return True
    else:
        return False


def _get_columns_to_retain_blocking(settings_dict, df):

    columns_to_retain = OrderedSet()
    columns_to_retain.add(settings_dict["unique_id_column_name"])
    if _retain_source_dataset_column(settings_dict, df):
        columns_to_retain.add(settings_dict["source_dataset_column_name"])
    for col in settings_dict["comparison_columns"]:
        cc = ComparisonColumn(col)
        for col_name in cc.columns_used:
            columns_to_retain.add(col_name)

    for c in settings_dict["additional_columns_to_retain"]:
        columns_to_retain.add(c)

    return columns_to_retain.as_list()


def _sql_gen_and_not_previous_rules(previous_rules: list):
    if previous_rules:
        # Note the isnull function is important here - otherwise
        # you filter out any records with nulls in the previous rules
        # meaning these comparisons get lost
        or_clauses = [f"ifnull(({r}), false)" for r in previous_rules]
        previous_rules = " OR ".join(or_clauses)
        return f"AND NOT ({previous_rules})"
    else:
        return ""


def _sql_gen_composite_unique_id(source_dataset_colname, unique_id_colname, l_or_r):
    return f"concat({l_or_r}.{source_dataset_colname}, '-__-',{l_or_r}.{unique_id_colname})"


def _sql_gen_where_condition(link_type, source_dataset_col, unique_id_col):

    if source_dataset_col:
        id_expr_l = _sql_gen_composite_unique_id(source_dataset_col, unique_id_col, "l")
        id_expr_r = _sql_gen_composite_unique_id(source_dataset_col, unique_id_col, "r")
    else:
        id_expr_l = f"l.{unique_id_col}"
        id_expr_r = f"r.{unique_id_col}"

    if link_type in ["link_and_dedupe", "dedupe_only"]:
        where_condition = f"where {id_expr_l} < {id_expr_r}"
    elif link_type == "link_only":
        where_condition = f"where {id_expr_l} < {id_expr_r} and l.{source_dataset_col} != r.{source_dataset_col}"

    return where_condition


def _sql_gen_block_using_rules(
    link_type: str,
    columns_to_retain: list,
    blocking_rules: list,
    unique_id_col: str = "unique_id",
    source_dataset_col: str = "source_dataset",
    table_name: str = "df",
):
    """Build a SQL statement that implements a list of blocking rules.

    The left and right tables are aliased as `l` and `r` respectively, so an example
    blocking rule would be `l.surname = r.surname AND l.forename = r.forename`.

    Args:
        link_type: One of 'link_only', 'dedupe_only', or 'link_and_dedupe'
        columns_to_retain: List of columns to keep in returned dataset
        blocking_rules: Each element of the list represents a blocking rule
        unique_id_col (str, optional): The name of the column containing the row's unique_id. Defaults to "unique_id".
        table_name (str, optional): Name of the table to dedupe (where (where `link_type` is `dedupe_only`). Defaults to "df".

    Returns:
        str: A SQL statement that implements the blocking rules
    """

    sql_select_expr = sql_gen_comparison_columns(columns_to_retain)
    where_condition = _sql_gen_where_condition(
        link_type, source_dataset_col, unique_id_col
    )

    sqls = []
    previous_rules = []
    for matchkey_number, rule in enumerate(blocking_rules):
        not_previous_rules_statement = _sql_gen_and_not_previous_rules(previous_rules)
        sql = f"""
        select
        {sql_select_expr},
        '{matchkey_number}' as match_key
        from {table_name} as l
        inner join {table_name} as r
        on
        {rule}
        {not_previous_rules_statement}
        {where_condition}
        """
        previous_rules.append(rule)
        sqls.append(sql)

    sql = "union all".join(sqls)

    return sql


def _sql_gen_cartesian_block(
    link_type: str,
    columns_to_retain: list,
    unique_id_col: str = "unique_id",
    source_dataset_col: str = "source_dataset",
    table_name: str = "df",
):
    """Build a SQL statement that performs a cartesian join.

    Args:
        link_type: One of 'link_only', 'dedupe_only', or 'link_and_dedupe'
        columns_to_retain: List of columns to keep in returned dataset
        unique_id_col (str, optional): The name of the column containing the row's unique_id. Defaults to "unique_id".

    Returns:
        str: A SQL statement that implements the join
    """

    sql_select_expr = sql_gen_comparison_columns(columns_to_retain)

    where_condition = _sql_gen_where_condition(
        link_type, source_dataset_col, unique_id_col
    )

    sql = f"""
    select
    {sql_select_expr},
    '0' as match_key
    from {table_name} as l
    cross join {table_name} as r
    {where_condition}
    """

    return sql


@typechecked
def block_using_rules(settings: dict, df: DataFrame, spark: SparkSession):
    """Apply a series of blocking rules to create a dataframe of record comparisons. If no blocking rules provided, performs a cartesian join.

    Args:
        settings (dict): A splink settings dictionary
        df (DataFrame): Spark dataframe to block - if linking multiple datasets, assumes dataframes have already been vertically concatenated
        spark (SparkSession): The pyspark.sql.session.SparkSession

    Returns:
        pyspark.sql.dataframe.DataFrame: A dataframe of each record comparison
    """
    df.createOrReplaceTempView("df")
    columns_to_retain = _get_columns_to_retain_blocking(settings, df)
    unique_id_col = settings["unique_id_column_name"]
    if settings["link_type"] == "dedupe_only":
        source_dataset_col = None
    else:
        source_dataset_col = settings["source_dataset_column_name"]
    link_type = settings["link_type"]

    if "blocking_rules" not in settings or len(settings["blocking_rules"]) == 0:
        sql = _sql_gen_cartesian_block(
            link_type, columns_to_retain, unique_id_col, source_dataset_col
        )
    else:
        rules = settings["blocking_rules"]
        sql = _sql_gen_block_using_rules(
            link_type, columns_to_retain, rules, unique_id_col, source_dataset_col
        )

    logger.debug(_format_sql(sql))

    df_comparison = spark.sql(sql)

    return df_comparison
