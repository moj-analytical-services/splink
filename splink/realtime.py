from pathlib import Path
from typing import Any, Dict

from .internals.database_api import DatabaseAPISubClass
from .internals.misc import ascii_uid
from .internals.pipeline import CTEPipeline
from .internals.predict import (
    predict_from_comparison_vectors_sqls_using_settings,
)
from .internals.settings_creator import SettingsCreator
from .internals.splink_dataframe import SplinkDataFrame

__all__ = [
    "compare_records",
]


_sql_used_for_compare_records_cache = {"sql": None, "uid": None}

def compare_records(
    record_1: Dict[str, Any],
    record_2: Dict[str, Any],
    settings: SettingsCreator | dict[str, Any] | Path | str,
    db_api: DatabaseAPISubClass,
    use_sql_from_cache: bool = True,
) -> SplinkDataFrame:
    """Compare two records and compute similarity scores without requiring a Linker.
    Assumes any required term frequency values are provided in the input records.

    Args:
        record_1 (dict): First record to compare
        record_2 (dict): Second record to compare
        db_api (DatabaseAPISubClass): Database API to use for computations

    Returns:
        SplinkDataFrame: Comparison results
    """
    global _sql_used_for_compare_records_cache

    uid = ascii_uid(8)

    if isinstance(record_1, dict):
        to_register_left = [record_1]
    else:
        to_register_left = record_1

    if isinstance(record_2, dict):
        to_register_right = [record_2]
    else:
        to_register_right = record_2

    df_records_left = db_api.register_table(
        to_register_left,
        f"__splink__compare_records_left_{uid}",
        overwrite=True,
    )
    df_records_left.templated_name = "__splink__compare_records_left"

    df_records_right = db_api.register_table(
        to_register_right,
        f"__splink__compare_records_right_{uid}",
        overwrite=True,
    )
    df_records_right.templated_name = "__splink__compare_records_right"

    if _sql_used_for_compare_records_cache["sql"] is not None and use_sql_from_cache:
        sql = _sql_used_for_compare_records_cache["sql"]
        uid_in_sql = _sql_used_for_compare_records_cache["uid"]
        sql = sql.replace(uid_in_sql, uid)
        return db_api._execute_sql_against_backend(sql)


    if not isinstance(settings, SettingsCreator):
        settings_creator = SettingsCreator.from_path_or_dict(settings)
    else:
        settings_creator = settings


    settings_obj = settings_creator.get_settings(db_api.sql_dialect.sql_dialect_str)

    pipeline = CTEPipeline([df_records_left, df_records_right])


    cols_to_select = settings_obj._columns_to_select_for_blocking

    select_expr = ", ".join(cols_to_select)
    sql = f"""
    select {select_expr}, 0 as match_key
    from __splink__compare_records_left as l
    cross join __splink__compare_records_right as r
    """
    pipeline.enqueue_sql(sql, "__splink__compare_two_records_blocked")

    cols_to_select = (
        settings_obj._columns_to_select_for_comparison_vector_values
    )
    select_expr = ", ".join(cols_to_select)
    sql = f"""
    select {select_expr}
    from __splink__compare_two_records_blocked
    """
    pipeline.enqueue_sql(sql, "__splink__df_comparison_vectors")

    sqls = predict_from_comparison_vectors_sqls_using_settings(
        settings_obj,
        sql_infinity_expression=db_api.sql_dialect.infinity_expression,
    )
    pipeline.enqueue_list_of_sqls(sqls)

    predictions = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    _sql_used_for_compare_records_cache["sql"] = predictions.sql_used_to_create
    _sql_used_for_compare_records_cache["uid"] = uid

    return predictions
