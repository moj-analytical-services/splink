from __future__ import annotations

from typing import Any, List, Sequence

from splink.internals.charts import (
    ChartReturnType,
)
from splink.internals.charts import (
    completeness_chart as records_to_completeness_chart,
)
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.input_column import InputColumn
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.vertically_concatenate import vertically_concatenate_sql


def completeness_data(
    splink_df_dict: dict[str, SplinkDataFrame],
    db_api: DatabaseAPISubClass,
    cols: List[str] = None,
    table_names_for_chart: List[str] = None,
) -> list[dict[str, Any]]:
    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict, salting_required=False, source_dataset_input_column=None
    )

    pipeline.enqueue_sql(sql, "__splink__df_concat")

    # use an internal name for source_dataset, as if it already exists in dataframe
    # we may run into ambiguous column issues. This name unlikely to clash
    internal_source_colname = "__completeness_source_dataset"

    # In the case of a single input dataframe, a source_dataset column
    # will not have been created, create one
    first_df = next(iter(splink_df_dict.values()))
    if len(splink_df_dict) == 1:
        sql = f"""
        select '{first_df.physical_name}' as {internal_source_colname}, *
        from __splink__df_concat
        """

    else:
        sql = (
            f"select *, source_dataset AS {internal_source_colname} "
            f"from __splink__df_concat"
        )

    pipeline.enqueue_sql(sql, "__splink__df_concat_with_source_dataset")

    if cols is None:
        cols_as_input_col = first_df.columns
    else:
        sqlglot_dialect = db_api.sql_dialect.sqlglot_dialect
        cols_as_input_col = [
            InputColumn(c, sqlglot_dialect_str=sqlglot_dialect) for c in cols
        ]

    sqls = []
    for col in cols_as_input_col:
        quoted_col = col.name
        unquoted_col = col.unquote().name

        sql = f"""
        (select
            {internal_source_colname} AS source_dataset,
            '{unquoted_col}' as column_name,
            count(*) - count({quoted_col}) as total_null_rows,
            count(*) as total_rows_inc_nulls,
            cast(count({quoted_col})*1.0/count(*) as float) as completeness
        from __splink__df_concat_with_source_dataset
        group by {internal_source_colname}
        order by count(*) desc)
        """
        sqls.append(sql)

    sql = " union all ".join(sqls)

    pipeline.enqueue_sql(sql, "__splink__df_all_column_completeness")

    # Replace table names with something user-friendly
    if table_names_for_chart is None:
        table_names_for_chart = [
            f"input_data_{i+1}" for i in range(len(splink_df_dict))
        ]
    physical_names = [df.physical_name for df in splink_df_dict.values()]
    whens = " ".join(
        [
            f"WHEN source_dataset = '{table}' THEN '{name}'"
            for table, name in zip(physical_names, table_names_for_chart)
        ]
    )
    case_when = f"CASE {whens} END"

    sql = f"""
    select {case_when} as source_dataset,
    column_name,
    total_null_rows,
    total_rows_inc_nulls,
    completeness
    from __splink__df_all_column_completeness
    """

    pipeline.enqueue_sql(sql, "__splink__df_all_column_completeness_renames")
    df = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df.as_record_dict()


def completeness_chart(
    table_or_tables: Sequence[AcceptableInputTableType],
    db_api: DatabaseAPISubClass,
    cols: List[str] = None,
    table_names_for_chart: List[str] = None,
) -> ChartReturnType:
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
    """

    splink_df_dict = db_api.register_multiple_tables(table_or_tables)
    records = completeness_data(splink_df_dict, db_api, cols, table_names_for_chart)
    return records_to_completeness_chart(records)
