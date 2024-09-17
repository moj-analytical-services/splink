from __future__ import annotations

from typing import Any, Dict, List, Union

import pyarrow as pa

from splink.internals.comparison_creator import ComparisonCreator
from splink.internals.comparison_level_creator import ComparisonLevelCreator
from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.misc import ascii_uid, ensure_is_list
from splink.internals.pipeline import CTEPipeline
from splink.internals.settings import ColumnInfoSettings


def is_in_level(
    comparison_level: ComparisonLevelCreator,
    literal_values: Union[Dict[str, Any], List[Dict[str, Any]], pa.Table],
    db_api: DatabaseAPISubClass,
) -> bool | List[bool]:
    sqlglot_dialect = db_api.sql_dialect.sqlglot_dialect
    sql_cond = comparison_level.get_comparison_level(sqlglot_dialect).sql_condition
    if sql_cond == "ELSE":
        sql_cond = "TRUE"

    table_name = f"__splink__temp_table_{ascii_uid(8)}"
    if isinstance(literal_values, pa.Table):
        db_api._table_registration(literal_values, table_name)
    else:
        literal_values_list = ensure_is_list(literal_values)
        db_api._table_registration(literal_values_list, table_name)

    sql_to_evaluate = f"SELECT {sql_cond} as result FROM {table_name}"

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql_to_evaluate, "__splink__is_in_level")
    res = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    db_api.delete_table_from_database(table_name)

    result = [bool(row["result"]) for row in res.as_record_dict()]
    return result[0] if isinstance(literal_values, dict) else result


def comparison_vector_value(
    comparison: ComparisonCreator,
    literal_values: Union[Dict[str, Any], List[Dict[str, Any]], pa.Table],
    db_api: DatabaseAPISubClass,
) -> Dict[str, Any] | List[Dict[str, Any]]:
    sqlglot_dialect = db_api.sql_dialect.sqlglot_dialect

    mock_column_info_settings = ColumnInfoSettings(
        bayes_factor_column_prefix="bm_",
        term_frequency_adjustment_column_prefix="tf_",
        comparison_vector_value_column_prefix="cv_",
        unique_id_column_name="unique_id",
        _source_dataset_column_name="dataset",
        _source_dataset_column_name_is_required=False,
        sql_dialect=db_api.sql_dialect.sql_dialect_str,
    )

    comparison_internal = comparison.get_comparison(sqlglot_dialect)
    comparison_internal.column_info_settings = mock_column_info_settings
    case_statement = comparison_internal._case_statement

    table_name = f"__splink__temp_table_{ascii_uid(8)}"
    if isinstance(literal_values, pa.Table):
        db_api._table_registration(literal_values, table_name)
    else:
        literal_values_list = ensure_is_list(literal_values)
        db_api._table_registration(literal_values_list, table_name)

    sql_to_evaluate = f"SELECT {case_statement}  FROM {table_name}"

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql_to_evaluate, "__splink__compute_cvv")
    res = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    db_api.delete_table_from_database(table_name)

    result_dicts = res.as_record_dict()

    instantiated_levels = comparison_internal.comparison_levels
    cvv_label_lookup = {
        level.comparison_vector_value: level.label_for_charts
        for level in instantiated_levels
    }

    result_key = next(iter(result_dicts[0]))

    output = [
        {
            "comparison_vector_value": row[result_key],
            "label_for_charts": cvv_label_lookup.get(row[result_key], ""),
        }
        for row in result_dicts
    ]

    return output if isinstance(literal_values, (list, pa.Table)) else output[0]
