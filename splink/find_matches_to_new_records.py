from typing import TYPE_CHECKING

from .input_column import InputColumn

if TYPE_CHECKING:
    from .linker import Linker
    from .splink_dataframe import SplinkDataFrame


def add_unique_id_and_source_dataset_cols_if_needed(
    linker: "Linker", new_records_df: "SplinkDataFrame"
):
    cols = new_records_df.columns
    cols = [c.unquote().name for c in cols]

    # Add source dataset column to new records if required and not exists
    sds_sel_sql = ""
    if linker._settings_obj._source_dataset_column_name_is_required:
        sds_col = linker._settings_obj._source_dataset_column_name

        if sds_col not in cols:
            sds_sel_sql = f", 'new_record' as {sds_col}"

    # Add unique_id column to new records if not exists
    uid_sel_sql = ""
    uid_col = linker._settings_obj._unique_id_column_name
    uid_col = InputColumn(uid_col, linker._settings_obj)
    uid_col = uid_col.unquote().name
    if uid_col not in cols:
        uid_sel_sql = f", 'no_id_provided' as {uid_col}"

    sql = f"""
        select * {sds_sel_sql} {uid_sel_sql}
        from  __splink__df_new_records_with_tf_before_uid_fix
        """
    linker._enqueue_sql(sql, "__splink__df_new_records_with_tf")
