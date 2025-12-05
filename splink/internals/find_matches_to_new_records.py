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

    # Add source dataset column to new records if required and not exists
    sds_sel_sql = ""
    col_info = linker._settings_obj.column_info_settings
    sds_input_col = col_info.source_dataset_input_column
    if sds_input_col is not None:
        if sds_input_col not in input_cols:
            sds_col_name = col_info.source_dataset_column_name
            sds_sel_sql = f", 'new_record' as {sds_col_name}"

    # Add unique_id column to new records if not exists
    uid_sel_sql = ""
    uid_col = col_info.unique_id_input_column

    if uid_str is not None:
        id_literal = uid_str
    else:
        id_literal = "no_id_provided"

    if uid_col not in input_cols:
        uid_sel_sql = f", '{id_literal}' as {uid_col.name}"

    sql = f"""
        select * {sds_sel_sql} {uid_sel_sql}
        from  {in_tablename}
        """
    pipeline.enqueue_sql(sql, out_tablename)
    return pipeline
