import logging
from collections import OrderedDict

# For type hints. Try except to ensure the sql_gen functions even if spark doesn't exist.
try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None


from .logging_utils import _format_sql
from .check_types import check_types

logger = logging.getLogger(__name__)

def sql_gen_comparison_columns(columns:list) -> str:
    """Build SQL expression that renames columns and sets them aside each other for comparisons

    Args:
        columns (list): [description]

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

def _get_columns_to_retain_blocking(settings):

    # Use ordered dict as an ordered set - i.e. to make sure we don't have duplicate cols to retain

    # That means we're only interested in the keys so we set values to None

    columns_to_retain = OrderedDict()
    columns_to_retain[settings["unique_id_column_name"]] = None

    for c in settings["comparison_columns"]:
        if "col_name" in c:
            columns_to_retain[c["col_name"]] = None
        if "custom_columns_used" in c:
            for c2 in c["custom_columns_used"]:
                columns_to_retain[c2] = None

    for c in settings["additional_columns_to_retain"]:
        columns_to_retain[c] = None

    return list(columns_to_retain.keys())

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

def _sql_gen_vertically_concatenate(columns_to_retain: list, table_name_l = "df_l", table_name_r = "df_r"):

    retain = ", ".join(columns_to_retain)

    sql = f"""
    select {retain}, 'left' as _source_table
    from {table_name_l}
    union all
    select {retain}, 'right' as _source_table
    from {table_name_r}
    """

    return sql

def _vertically_concatenate_datasets(df_l, df_r, settings, spark=None):

    columns_to_retain = _get_columns_to_retain_blocking(settings)
    sql = _sql_gen_vertically_concatenate(columns_to_retain)

    df_l.createOrReplaceTempView("df_l")
    df_r.createOrReplaceTempView("df_r")
    logger.debug(_format_sql(sql))
    df = spark.sql(sql)
    return df

def _sql_gen_block_using_rules(
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
        table_name_l (str, optional): Name of the left table to link (where `link_type` is `link_only` or `link_and_dedupe`). Defaults to "df_l".
        table_name_r (str, optional): Name of the right table to link (where `link_type` is `link_only` or `link_and_dedupe`). Defaults to "df_r".
        table_name_dedupe (str, optional): Name of the table to dedupe (where (where `link_type` is `dedupe_only`). Defaults to "df".

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
    elif link_type == "dedupe_only":
        where_condition = f"where l.{unique_id_col} < r.{unique_id_col}"
    elif link_type == "link_and_dedupe":
            # Where a record from left and right are being compared, you want the left record to end up in the _l fields,  and the right record to end up in _r fields.
            where_condition = f"where (l._source_table < r._source_table) or (l.{unique_id_col} < r.{unique_id_col} and l._source_table = r._source_table)"

    sqls = []
    previous_rules =[]
    for rule in blocking_rules:
        not_previous_rules_statement = _sql_gen_and_not_previous_rules(previous_rules)
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

@check_types
def block_using_rules(
    settings: dict,
    spark: SparkSession,
    df_l: DataFrame=None,
    df_r: DataFrame=None,
    df: DataFrame=None
):
    """Apply a series of blocking rules to create a dataframe of record comparisons. If no blocking rules provided, performs a cartesian join.

    Args:
        settings (dict): A splink settings dictionary
        spark (SparkSession): The pyspark.sql.session.SparkSession
        df_l (DataFrame, optional): Where `link_type` is `link_only` or `link_and_dedupe`, one of the two dataframes to link. Should be ommitted `link_type` is `dedupe_only`.
        df_r (DataFrame, optional): Where `link_type` is `link_only` or `link_and_dedupe`, one of the two dataframes to link. Should be ommitted `link_type` is `dedupe_only`.
        df (DataFrame, optional): Where `link_type` is `dedupe_only`, the dataframe to dedupe. Should be ommitted `link_type` is `link_only` or `link_and_dedupe`.

    Returns:
        pyspark.sql.dataframe.DataFrame: A dataframe of each record comparison
    """

    if "blocking_rules" not in settings or len(settings["blocking_rules"])==0:
        return cartesian_block(settings, spark, df_l, df_r, df)

    link_type = settings["link_type"]

    columns_to_retain = _get_columns_to_retain_blocking(settings)
    unique_id_col = settings["unique_id_column_name"]

    if link_type == "dedupe_only":
        df.createOrReplaceTempView("df")

    if link_type == "link_only":
        df_l.createOrReplaceTempView("df_l")
        df_r.createOrReplaceTempView("df_r")

    if link_type == "link_and_dedupe":
        df_concat = _vertically_concatenate_datasets(df_l, df_r, settings, spark=spark)
        columns_to_retain.append("_source_table")
        df_concat.createOrReplaceTempView("df")
        df_concat.persist()

    rules = settings["blocking_rules"]

    sql = _sql_gen_block_using_rules(link_type, columns_to_retain, rules, unique_id_col)

    logger.debug(_format_sql(sql))

    df_comparison = spark.sql(sql)

    if link_type == "link_and_dedupe":
        df_concat.unpersist()


    return df_comparison


def _sql_gen_cartesian_block(
    link_type: str,
    columns_to_retain: list,
    unique_id_col: str = "unique_id",
    table_name_l: str = "df_l",
    table_name_r: str = "df_r",
    table_name_dedupe: str = "df"
):
    """Build a SQL statement that performs a cartesian join.

    Args:
        link_type: One of 'link_only', 'dedupe_only', or 'link_and_dedupe'
        columns_to_retain: List of columns to keep in returned dataset
        unique_id_col (str, optional): The name of the column containing the row's unique_id. Defaults to "unique_id".
        table_name_l (str, optional): Name of the left table to link (where `link_type` is `link_only` or `link_and_dedupe`). Defaults to "df_l".
        table_name_r (str, optional): Name of the right table to link (where `link_type` is `link_only` or `link_and_dedupe`). Defaults to "df_r".
        table_name_dedupe (str, optional): Name of the table to dedupe (where (where `link_type` is `dedupe_only`). Defaults to "df".

    Returns:
        str: A SQL statement that implements the join
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
    elif link_type == "dedupe_only":
        where_condition = f"where l.{unique_id_col} < r.{unique_id_col}"
    elif link_type == "link_and_dedupe":
            # Where a record from left and right are being compared, you want the left record to end up in the _l fields,  and the right record to end up in _r fields.
            where_condition = f"where (l._source_table < r._source_table) or (l.{unique_id_col} < r.{unique_id_col} and l._source_table = r._source_table)"

    sql = f"""
    select
    {sql_select_expr}
    from {table_name_l} as l
    cross join {table_name_r} as r
    {where_condition}
    """

    return sql


def cartesian_block(
    settings: dict,
    spark: SparkSession,
    df_l: DataFrame=None,
    df_r: DataFrame=None,
    df: DataFrame=None
):
    """Apply a cartesian join to create a dataframe of record comparisons.

    Args:
        settings (dict): A sparklink settings dictionary
        spark (SparkSession): The pyspark.sql.session.SparkSession
        df_l (DataFrame, optional): Where `link_type` is `link_only` or `link_and_dedupe`, one of the two dataframes to link. Should be ommitted `link_type` is `dedupe_only`.
        df_r (DataFrame, optional): Where `link_type` is `link_only` or `link_and_dedupe`, one of the two dataframes to link. Should be ommitted `link_type` is `dedupe_only`.
        df (DataFrame, optional): Where `link_type` is `dedupe_only`, the dataframe to dedupe. Should be ommitted `link_type` is `link_only` or `link_and_dedupe`.

    Returns:
        pyspark.sql.dataframe.DataFrame: A dataframe of each record comparison
    """

    link_type = settings["link_type"]

    columns_to_retain = _get_columns_to_retain_blocking(settings)
    unique_id_col = settings["unique_id_column_name"]

    if link_type == "dedupe_only":
        df.createOrReplaceTempView("df")

    if link_type == "link_only":
        df_l.createOrReplaceTempView("df_l")
        df_r.createOrReplaceTempView("df_r")

    if link_type == "link_and_dedupe":
        columns_to_retain.append("_source_table")
        df_concat = _vertically_concatenate_datasets(df_l, df_r, settings, spark=spark)
        df_concat.createOrReplaceTempView("df")
        df_concat.persist()

    sql = _sql_gen_cartesian_block(link_type, columns_to_retain, unique_id_col)

    logger.debug(_format_sql(sql))

    df_comparison = spark.sql(sql)

    if link_type == "link_and_dedupe":
        df_concat.unpersist()

    return df_comparison
