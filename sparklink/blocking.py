import logging

from formatlog import format_sql
from sql import comparison_columns_select_expr

log = logging.getLogger(__name__)

def block_using_rules(df, blocking_rules, spark=None, unique_id_col="unique_id"):

    columns = list(df.columns)

    sql_select_expr = comparison_columns_select_expr(df)
    df.registerTempTable("df")

    sqls = []
    for rule in blocking_rules:
        sql = f"""
        select
        {sql_select_expr}
        from df as l
        left join df as r
        on
        {rule}
        where l.{unique_id_col} < r.{unique_id_col}
        """
        sqls.append(sql)

    # Note the 'union' function in pyspark > 2.0 is not the same thing as union in a sql statement
    sql = "union all".join(sqls)
    df_comparison = spark.sql(sql)
    log.debug(format_sql(sql))

    df_comparison = df_comparison.dropDuplicates()

    return df_comparison


def cartestian_block(df, spark=None,  unique_id_col="unique_id"):
    columns = list(df.columns)

    sql_select_expr = comparison_columns_select_expr(df)
    df.registerTempTable("df")

    sql = f"""
    select
    {sql_select_expr}
    from df as l
    cross join df as r
    where l.{unique_id_col} < r.{unique_id_col}
    """


    # Note the 'union' function in pyspark > 2.0 is not the same thing as union in a sql statement
    df_comparison = spark.sql(sql)
    log.debug(format_sql(sql))

    return df_comparison
