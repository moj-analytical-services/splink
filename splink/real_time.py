from typing import Any, Dict

from .internals import similarity_analysis
from .internals.completeness import completeness_chart
from .internals.database_api import DatabaseAPISubClass
from .internals.pipeline import CTEPipeline
from .internals.profile_data import profile_columns
from .internals.splink_dataframe import SplinkDataFrame

__all__ = [
    "compare_records",
]


def compare_records(
    record_1: Dict[str, Any],
    record_2: Dict[str, Any],
    db_api: DatabaseAPISubClass,
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

    pipeline = CTEPipeline([df_records_left, df_records_right])

    # Cross join the records
    cols_to_select = [c.name for c in df_records_left.columns]
    select_expr = ", ".join(cols_to_select)

    sql = f"""
    select {select_expr}, 0 as match_key
    from __splink__compare_records_left_{uid} as l
    cross join __splink__compare_records_right_{uid} as r
    """
    pipeline.enqueue_sql(sql, "__splink__compare_records_blocked")

    # Select comparison columns
    sql = f"""
    select {select_expr}
    from __splink__compare_records_blocked
    """
    pipeline.enqueue_sql(sql, "__splink__df_comparison_vectors")

    # Execute pipeline and return results
    predictions = db_api.sql_pipeline_to_splink_dataframe(pipeline, use_cache=False)

    return predictions
