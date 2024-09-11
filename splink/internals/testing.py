from typing import Any, Dict

import pyarrow as pa

from splink.internals.comparison_creator import ComparisonCreator
from splink.internals.comparison_level_creator import ComparisonLevelCreator
from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.pipeline import CTEPipeline


def is_in_level(
    comparison_level: ComparisonLevelCreator,
    literal_values: Dict[str, Any],
    db_api: DatabaseAPISubClass,
) -> bool:
    sqlglot_dialect = db_api.sql_dialect.sqlglot_name
    sql_cond = comparison_level.get_comparison_level(sqlglot_dialect).sql_condition
    if sql_cond == "ELSE":
        return True

    pa_table = pa.Table.from_pydict({k: [v] for k, v in literal_values.items()})

    table_name = "__splink__temp_table"
    db_api._table_registration(pa_table, table_name)

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
    creator_levels = comparison.create_comparison_levels()
    instantiated_levels = comparison.get_comparison(sqlglot_dialect).comparison_levels

    levels = [
        {
            "creator": c,
            "comparison_vector_value": inst.comparison_vector_value,
            "label_for_charts": inst.label_for_charts,
        }
        for c, inst in zip(creator_levels, instantiated_levels)
    ]

    for d in levels:
        in_level = is_in_level(d["creator"], literal_values, db_api)
        if in_level:
            return {
                "comparison_vector_value": d["comparison_vector_value"],
                "label_for_charts": d["label_for_charts"],
            }

    return {}
