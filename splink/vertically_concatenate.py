import logging

logger = logging.getLogger(__name__)


def vertically_concatente(df_dict):

    # Use column order from first table in dict
    df_obj = next(iter(df_dict.values()))
    columns = df_obj.columns_escaped

    select_columns_sql = ", ".join(columns)

    sqls_to_union = []
    for df_obj in df_dict.values():
        sql = f"""
        select '{df_obj.templated_name}' as source_dataset, {select_columns_sql}
        from {df_obj.physical_name}
        """
        sqls_to_union.append(sql)
    sql = " UNION ALL ".join(sqls_to_union)

    logger.debug("\n" + sql)

    return sql
