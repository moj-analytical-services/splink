from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from splink.internals.accuracy import _select_found_by_blocking_rules
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.misc import ascii_uid
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import (
    predict_from_comparison_vectors_sqls_using_settings,
)
from splink.internals.settings_creator import SettingsCreator
from splink.internals.splink_dataframe import SplinkDataFrame


class SQLCache:
    def __init__(self) -> None:
        self._cache: dict[str, tuple[str, str]] = {}

    def get(
        self,
        cache_key: str,
        new_uid: str,
    ) -> str | None:
        sql, cached_uid = self._cache.get(cache_key, (None, None))
        if cached_uid is not None:
            # sql will always be a str if cached_id is (and thus not None)
            sql = cast(str, sql).replace(cached_uid, new_uid)
        return sql

    def set(
        self,
        cache_key: str,
        sql: str,
        uid: str,
    ) -> None:
        self._cache[cache_key] = (sql, uid)


_sql_cache = SQLCache()


def compare_records(
    record_1: dict[str, Any] | AcceptableInputTableType,
    record_2: dict[str, Any] | AcceptableInputTableType,
    settings: SettingsCreator | dict[str, Any] | Path | str,
    db_api: DatabaseAPISubClass,
    sql_cache_key: str | None = None,
    include_found_by_blocking_rules: bool = False,
    join_condition: str = "1=1",
) -> SplinkDataFrame:
    """Compare two records and compute similarity scores without requiring a Linker.
    Assumes any required term frequency values are provided in the input records.

    Args:
        record_1 (dict): First record to compare
        record_2 (dict): Second record to compare
        settings (SettingsCreator, dict, Path, str): Model settings, or path to
            a saved model
        db_api (DatabaseAPISubClass): Database API to use for computations
        sql_cache_key (str): Use cached SQL if available, rather than re-constructing,
            stored under this cache key. If None, do not retrieve sql, or cache it.
            Default None.
        include_found_by_blocking_rules (bool): Include a column indicating whether
            or not the pairs of records would have been picked up by the supplied
            blocking rules. Defaults to False.
        join_condition (str): A SQL expression in terms of the tables 'l' and 'r',
            which is used to filter which records are compared.
            Defaults to '1=1' (meaning all pairs of records remain)

    Returns:
        SplinkDataFrame: Comparison results
    """
    global _sql_cache

    uid = ascii_uid(8)

    if isinstance(record_1, dict):
        to_register_left: AcceptableInputTableType = [record_1]
    else:
        to_register_left = record_1

    if isinstance(record_2, dict):
        to_register_right: AcceptableInputTableType = [record_2]
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

    if sql_cache_key:
        cached_sql = _sql_cache.get(sql_cache_key, uid)
        if cached_sql:
            return db_api._sql_to_splink_dataframe(
                cached_sql,
                templated_name="__splink__realtime_compare_records",
                physical_name=f"__splink__realtime_compare_records_{uid}",
            )

    if not isinstance(settings, SettingsCreator):
        settings_creator = SettingsCreator.from_path_or_dict(settings)
    else:
        settings_creator = settings

    settings_obj = settings_creator.get_settings(db_api.sql_dialect.sql_dialect_str)

    settings_obj._retain_matching_columns = True
    settings_obj._retain_intermediate_calculation_columns = True

    pipeline = CTEPipeline([df_records_left, df_records_right])

    cols_to_select = settings_obj._columns_to_select_for_blocking

    select_expr = ", ".join(cols_to_select)
    sql = f"""
    select {select_expr}, 0 as match_key
    from __splink__compare_records_left as l
    join __splink__compare_records_right as r
    on {join_condition}
    """
    pipeline.enqueue_sql(sql, "__splink__compare_two_records_blocked")

    cols_to_select = settings_obj._columns_to_select_for_comparison_vector_values
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

    if include_found_by_blocking_rules:
        br_col = _select_found_by_blocking_rules(settings_obj)
        sql = f"""
        select *, {br_col}
        from __splink__df_predict
        """

        pipeline.enqueue_sql(sql, "__splink__found_by_blocking_rules")

    predictions = db_api.sql_pipeline_to_splink_dataframe(pipeline)
    if sql_cache_key:
        _sql_cache.set(sql_cache_key, predictions.sql_used_to_create, uid)

    return predictions
