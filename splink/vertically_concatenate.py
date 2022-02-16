import logging

from .format_sql import format_sql

logger = logging.getLogger(__name__)


def vertically_concatente(df_dict, execute_sql):

    # Use column order from first table in dict
    df_obj = next(iter(df_dict.values()))
    columns = df_obj.columns_escaped

    select_columns_sql = ", ".join(columns)

    sqls_to_union = []
    for df_obj in df_dict.values():
        sql = f"""
        select '{df_obj.df_name}' as source_dataset, {select_columns_sql}
        from {df_obj.df_name}
        """
        sqls_to_union.append(sql)
    sql = " UNION ALL ".join(sqls_to_union)

    sql = format_sql(sql)
    logger.debug("\n" + sql)

    return execute_sql(sql, df_dict, "__splink__df_concat")
