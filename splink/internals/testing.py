from typing import Any, Dict

from splink.internals.comparison_creator import ComparisonCreator
from splink.internals.comparison_level_creator import ComparisonLevelCreator
from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.misc import ascii_uid
from splink.internals.pipeline import CTEPipeline
from splink.internals.settings import ColumnInfoSettings


def is_in_level(
    comparison_level: ComparisonLevelCreator,
    literal_values: Dict[str, Any],
    db_api: DatabaseAPISubClass,
) -> bool:
    sqlglot_dialect = db_api.sql_dialect.sqlglot_name
    sql_cond = comparison_level.get_comparison_level(sqlglot_dialect).sql_condition
    if sql_cond == "ELSE":
        return True

    table_name = f"__splink__temp_table_{ascii_uid(8)}"
    db_api._table_registration([literal_values], table_name)

    sql_to_evaluate = f"SELECT {sql_cond} as result FROM {table_name}"

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql_to_evaluate, "__splink__is_in_level")
    res = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    db_api.delete_table_from_database(table_name)

    return bool(res.as_record_dict()[0]["result"])


def compute_comparison_vector_value(
    comparison: ComparisonCreator,
    literal_values: Dict[str, Any],
    db_api: DatabaseAPISubClass,
) -> Dict[str, Any]:
    sqlglot_dialect = db_api.sql_dialect.sqlglot_name

    mock_column_info_settings = ColumnInfoSettings(
        bayes_factor_column_prefix="bm_",
        term_frequency_adjustment_column_prefix="tf_",
        comparison_vector_value_column_prefix="cv_",
        unique_id_column_name="unique_id",
        _source_dataset_column_name="dataset",
        _source_dataset_column_name_is_required=False,
        sql_dialect=db_api.sql_dialect,
    )

    comparison_internal = comparison.get_comparison(sqlglot_dialect)

    comparison_internal.column_info_settings = mock_column_info_settings

    case_statement = comparison_internal._case_statement

    table_name = f"__splink__temp_table_{ascii_uid(8)}"
    db_api._table_registration([literal_values], table_name)

    sql_to_evaluate = f"SELECT {case_statement}  FROM {table_name}"

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql_to_evaluate, "__splink__compute_cvv")
    res = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    db_api.delete_table_from_database(table_name)

    result_dict = res.as_record_dict()[0]
    first_column_name = next(iter(result_dict))

    result = result_dict[first_column_name]

    instantiated_levels = comparison_internal.comparison_levels

    cvv_label_lookup = {
        level.comparison_vector_value: level.label_for_charts
        for level in instantiated_levels
    }

    return {
        "comparison_vector_value": result,
        "label_for_charts": cvv_label_lookup.get(result, ""),
    }
