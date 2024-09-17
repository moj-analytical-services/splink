from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from splink.internals.pipeline import CTEPipeline

from .input_column import InputColumn

if TYPE_CHECKING:
    from splink.internals.linker import Linker
    from splink.internals.splink_dataframe import SplinkDataFrame


def add_unique_id_and_source_dataset_cols_if_needed(
    linker: "Linker",
    new_records_df: "SplinkDataFrame",
    pipeline: CTEPipeline,
    in_tablename: str,
    out_tablename: str,
    uid_str: Optional[str] = None,
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
        sqlglot_dialect_str=linker._settings_obj._sqlglot_dialect,
    )
    uid_col_name = uid_col.unquote().name

    if uid_str is not None:
        id_literal = uid_str
    else:
        id_literal = "no_id_provided"

    if uid_col_name not in cols:
        uid_sel_sql = f", '{id_literal}' as {uid_col.name}"

    sql = f"""
        select * {sds_sel_sql} {uid_sel_sql}
        from  {in_tablename}
        """
    pipeline.enqueue_sql(sql, out_tablename)
    return pipeline
