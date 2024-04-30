from typing import List

from ..charts import completeness_chart as records_to_completeness_chart
from ..database_api import DatabaseAPISubClass
from ..input_column import InputColumn
from ..pipeline import CTEPipeline
from ..vertically_concatenate import vertically_concatenate_sql


def completeness_data(
    splink_df_dict, db_api: DatabaseAPISubClass, cols: List[str] = None
):
    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict, salting_required=False, source_dataset_column_name=None
    )

    pipeline.enqueue_sql(sql, "__splink__df_concat")

    if cols is None:
        first_df = next(iter(splink_df_dict.values()))
        cols = first_df.columns
    else:
        cols = [InputColumn(c).name for c in cols]

    if len(splink_df_dict) == 1:
        source_name = "'input_data'"
    else:
        source_name = "source_dataset"

    sqls = []
    for col in cols:
        quoted_col = col.name
        unquoted_col = col.unquote().name

        sql = f"""
        (select
            {source_name} as source_dataset,
            '{unquoted_col}' as column_name,
            count(*) - count({quoted_col}) as total_null_rows,
            count(*) as total_rows_inc_nulls,
            cast(count({quoted_col})*1.0/count(*) as float) as completeness
        from __splink__df_concat
        group by source_dataset
        order by count(*) desc)
        """
        sqls.append(sql)

    sql = " union all ".join(sqls)

    pipeline.enqueue_sql(sql, "__splink__df_all_column_completeness")
    df = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df.as_record_dict()


def completeness_chart(
    table_or_tables,
    db_api: DatabaseAPISubClass,
    cols: list[str] = None,
    table_names_for_chart: list[str] = None,
):
    """Generate a summary chart of data completeness (proportion of non-nulls) of
    columns in each of the input table or tables. By default, completeness is assessed
    for all columns in the input data.

    Args:
        table_or_tables: A single table or a list of tables of data
        db_api (DatabaseAPISubClass): The backend database API to use
        cols (List[str], optional): List of column names to calculate completeness. If
            none, all columns will be computed. Default to None.
        table_names_for_chart: A list of names.  Must be the same length as
            table_or_tables.


        ```
    """
    if table_names_for_chart is None:
        table_names_for_chart = [
            f"input_data_{i+1}" for i in range(len(table_or_tables))
        ]
    splink_df_dict = db_api.register_multiple_tables(
        table_or_tables, input_aliases=table_names_for_chart
    )
    records = completeness_data(splink_df_dict, db_api, cols)
    return records_to_completeness_chart(records)
