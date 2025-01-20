from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from weakref import ref

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
    def __init__(self):
        self._cache = {}

    # TODO: if we have path/string, do we want to think about behaviour if underlying
    # file changes between calls?
    def get(
        self,
        settings: SettingsCreator | dict[str, Any] | Path | str,
        new_uid: str,
        *,
        sql_dialect_str: str,
    ) -> str | None:
        settings_id = self._cache_id(settings, sql_dialect_str)
        if settings_id not in self._cache:
            return None
        sql, cached_uid, settings_ref = self._cache[settings_id]
        # if reference is dead, delete cache entry and return nowt
        if settings_ref() is None:
            del self._cache[settings_id]
            return None

        if cached_uid:
            sql = sql.replace(cached_uid, new_uid)
        return sql

    def set(
        self,
        settings: SettingsCreator | dict[str, Any] | Path | str,
        sql: str | None,
        uid: str | None,
        *,
        sql_dialect_str: str,
    ) -> None:
        if sql is not None:
            settings_id = self._cache_id(settings, sql_dialect_str)
            # kind of hacky
            # allows us to not need to special-case retrieval - will appear as though
            # weakref is always live, so don't need to intervene
            settings_ref = (
                ref(settings)
                if isinstance(settings, SettingsCreator)
                else (lambda: True)
            )
            self._cache[settings_id] = (sql, uid, settings_ref)

    @staticmethod
    def _cache_id(
        settings: SettingsCreator | dict[str, Any] | Path | str, sql_dialect_str: str
    ) -> str:
        if isinstance(settings, SettingsCreator):
            return str(id(settings))
        if isinstance(settings, str):
            return settings
        if isinstance(settings, Path):
            return str(settings)
        # we have a dict
        try:
            key = json.dumps(settings)
        except TypeError:
            settings_dict = SettingsCreator(**settings).create_settings_dict(
                sql_dialect_str=sql_dialect_str
            )
            key = json.dumps(settings_dict)
        return key


_sql_cache = SQLCache()


def compare_records(
    record_1: dict[str, Any] | AcceptableInputTableType,
    record_2: dict[str, Any] | AcceptableInputTableType,
    settings: SettingsCreator | dict[str, Any] | Path | str,
    db_api: DatabaseAPISubClass,
    use_sql_from_cache: bool = True,
    include_found_by_blocking_rules: bool = False,
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
    global _sql_cache

    uid = ascii_uid(8)
    sql_dialect_str = db_api.sql_dialect.sql_dialect_str

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

    if use_sql_from_cache:
        cached_sql = _sql_cache.get(settings, uid, sql_dialect_str=sql_dialect_str)
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
    cross join __splink__compare_records_right as r
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
    _sql_cache.set(
        settings, predictions.sql_used_to_create, uid, sql_dialect_str=sql_dialect_str
    )

    return predictions
