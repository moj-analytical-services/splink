from __future__ import annotations

from typing import TYPE_CHECKING

from .input_column import InputColumn
from .pipeline import CTEPipeline

if TYPE_CHECKING:
    from .linker import Linker
    from .splink_dataframe import SplinkDataFrame


def add_unique_id_and_source_dataset_cols_if_needed(
    linker: "Linker", new_records_df: "SplinkDataFrame", pipeline: CTEPipeline
) -> CTEPipeline:
    input_cols: list[InputColumn] = new_records_df.columns
    cols: list[str] = [c.unquote().name for c in input_cols]

    # Add source dataset column to new records if required and not exists
    sds_sel_sql = ""
    if sds_col := linker._settings_obj.column_info_settings.source_dataset_column_name:
        if sds_col not in cols:
            sds_sel_sql = f", 'new_record' as {sds_col}"

    # Add unique_id column to new records if not exists
    uid_sel_sql = ""
    uid_col_name = linker._settings_obj.column_info_settings.unique_id_column_name
    uid_col = InputColumn(
        uid_col_name,
        column_info_settings=linker._settings_obj.column_info_settings,
        sql_dialect=linker._settings_obj._sql_dialect,
    )
    uid_col_name = uid_col.unquote().name
    if uid_col_name not in cols:
        uid_sel_sql = f", 'no_id_provided' as {uid_col.name}"

    sql = f"""
        select * {sds_sel_sql} {uid_sel_sql}
        from  __splink__df_new_records_with_tf_before_uid_fix
        """
    pipeline.enqueue_sql(sql, "__splink__df_new_records_with_tf")
    return pipeline
