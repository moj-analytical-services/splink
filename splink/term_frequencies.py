# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/splink/pull/107

import logging

from .format_sql import format_sql
from .misc import escape_column

logger = logging.getLogger(__name__)


def colname_to_tf_tablename(colname):
    return f"__splink__df_tf_{colname}"


def sql_gen_term_frequencies(column_name, table_name="__splink__df_concat"):

    tf_col_name = escape_column(f"tf_{column_name}")
    col_name = escape_column(column_name)

    sql = f"""
    select
    {col_name}, cast(count(*) as double) / (select
        count({col_name}) as total from {table_name}) as {tf_col_name}
    from {table_name}
    where {col_name} is not null
    group by {col_name}
    """

    return sql


def join_tf_to_input_df(settings_obj, df_dict, execute_sql):

    tf_cols = settings_obj._term_frequency_columns

    df_cols = df_dict["__splink__df_concat"].columns

    select_cols = []
    for df_col in df_cols:
        df_col_esc = escape_column(df_col)
        select_cols.append(f"df.{df_col_esc}")
        if df_col in tf_cols:
            tbl = colname_to_tf_tablename(df_col)
            tf_col = escape_column(f"tf_{df_col}")
            select_cols.append(f"{tbl}.{tf_col}")

    select_cols = ", ".join(select_cols)

    templ = "left join {tbl} on df.{col} = {tbl}.{col}"

    left_joins = [
        templ.format(tbl=colname_to_tf_tablename(col), col=escape_column(col))
        for col in tf_cols
    ]
    left_joins = " ".join(left_joins)

    sql = f"""
    select {select_cols}
    from __splink__df_concat as df
    {left_joins}
    """

    sql = format_sql(sql)
    logger.debug("\n" + sql)

    return execute_sql(sql, df_dict, "__splink__df_concat_with_tf")


def term_frequencies_dict(settings_obj, df_dict, user_provided_tf_dict, execute_sql):
    """Compute the term frequencies of the required columns and add to the dataframe.

    Returns:
        Spark dataframe: A dataframe containing new columns representing the term frequencies
        of the corresponding values
    """

    tf_cols = settings_obj._term_frequency_columns

    output_dict = user_provided_tf_dict
    for tf_col in tf_cols:
        if colname_to_tf_tablename(tf_col) not in output_dict:
            sql = sql_gen_term_frequencies(tf_col)
            sql = format_sql(sql)
            logger.debug("\n" + sql)
            tf_df = execute_sql(sql, df_dict, f"__splink__df_tf_{tf_col}")
            output_dict = {**output_dict, **tf_df}

    return output_dict
