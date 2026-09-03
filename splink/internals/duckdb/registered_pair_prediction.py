"""DuckDB-specific registered-pair prediction.

This planner workaround is kept separate so it can be removed without changing the
normal prediction path.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from splink.internals.blocking import combine_unique_id_input_columns
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.exceptions import SplinkException
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import (
    predict_from_comparison_vectors_sqls_using_settings,
)
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.term_frequencies import (
    _join_tf_to_input_table_sql,
    append_term_frequencies_to_pipeline,
)
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql
from splink.internals.vertically_concatenate import vertically_concatenate_sql

if TYPE_CHECKING:
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


@dataclass
class _RegisteredPredictInputs:
    left: SplinkDataFrame
    right: SplinkDataFrame

    def drop(self) -> None:
        for dataframe in (self.right, self.left):
            dataframe.drop_table_from_database_and_remove_from_cache()


def _materialize_registered_pair_input(
    linker: Linker,
    blocked_pairs: SplinkDataFrame,
    side: Literal["l", "r"],
) -> SplinkDataFrame:
    column_info = linker._settings_obj.column_info_settings
    unique_id_columns = combine_unique_id_input_columns(
        column_info.source_dataset_input_column,
        column_info.unique_id_input_column,
    )
    uid_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "source")
    concat_sql = vertically_concatenate_sql(
        linker._input_tables_dict,
        source_dataset_input_column=column_info.source_dataset_input_column,
    )
    sql = f"""
    select source.*
    from ({concat_sql}) as source
    semi join {blocked_pairs.physical_name} as pairs
    on {uid_expr} = pairs.join_key_{side}
    """
    templated_name = f"__splink__df_registered_predict_input_{side}"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql, templated_name)
    return linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)


def _materialize_registered_pair_inputs(
    linker: Linker,
    blocked_pairs: SplinkDataFrame,
) -> _RegisteredPredictInputs:
    left = _materialize_registered_pair_input(linker, blocked_pairs, "l")
    try:
        right = _materialize_registered_pair_input(linker, blocked_pairs, "r")
    except Exception:
        left.drop_table_from_database_and_remove_from_cache()
        raise
    return _RegisteredPredictInputs(left=left, right=right)


def _enqueue_registered_inputs_with_tf(
    linker: Linker,
    pipeline: CTEPipeline,
    inputs: _RegisteredPredictInputs,
) -> tuple[str, str]:
    append_term_frequencies_to_pipeline(linker, pipeline)
    left_name = "__splink__df_registered_predict_input_with_tf_l"
    pipeline.enqueue_sql(
        _join_tf_to_input_table_sql(
            linker,
            inputs.left.templated_name,
            inputs.left,
        ),
        left_name,
    )
    if inputs.left.physical_name == inputs.right.physical_name:
        return left_name, left_name

    right_name = "__splink__df_registered_predict_input_with_tf_r"
    pipeline.enqueue_sql(
        _join_tf_to_input_table_sql(
            linker,
            inputs.right.templated_name,
            inputs.right,
        ),
        right_name,
    )
    return left_name, right_name


def predict_from_blocked_pairs_duckdb(
    linker: Linker,
    blocked_pairs: SplinkDataFrame,
    threshold_match_probability: float | None,
    threshold_match_weight: float | None,
    emit_warning: bool,
) -> SplinkDataFrame:
    if linker._sql_dialect_str != "duckdb":
        raise SplinkException(
            "Registered-pair source pruning is currently supported only by DuckDB."
        )

    registered_inputs = _materialize_registered_pair_inputs(linker, blocked_pairs)
    try:
        settings = linker._settings_obj
        pipeline = CTEPipeline(
            [blocked_pairs, registered_inputs.left, registered_inputs.right]
        )
        input_tablename_l, input_tablename_r = _enqueue_registered_inputs_with_tf(
            linker, pipeline, registered_inputs
        )

        start_time = time.time()
        pipeline.enqueue_list_of_sqls(
            compute_comparison_vector_values_from_id_pairs_sqls(
                settings._columns_to_select_for_blocking,
                settings._columns_to_select_for_comparison_vector_values,
                input_tablename_l=input_tablename_l,
                input_tablename_r=input_tablename_r,
                source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
                unique_id_input_column=settings.column_info_settings.unique_id_input_column,
                link_type=settings._link_type,
                sql_dialect_str=linker._sql_dialect_str,
            )
        )
        pipeline.enqueue_list_of_sqls(
            predict_from_comparison_vectors_sqls_using_settings(
                settings,
                threshold_match_probability,
                threshold_match_weight,
            )
        )
        predictions = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
        predict_time = time.time() - start_time
        logger.info(f"Predict time (post-blocking): {predict_time:.2f} seconds")
    finally:
        registered_inputs.drop()

    if emit_warning:
        linker._predict_warning()

    return predictions


def predict_from_registered_pairs_duckdb(
    linker: Linker,
    blocked_pairs_cache_key: str,
    threshold_match_probability: float | None,
    threshold_match_weight: float | None,
    emit_warning: bool,
) -> SplinkDataFrame:
    blocked_pairs = linker._intermediate_table_cache.get_with_logging(
        blocked_pairs_cache_key
    )
    return predict_from_blocked_pairs_duckdb(
        linker,
        blocked_pairs,
        threshold_match_probability,
        threshold_match_weight,
        emit_warning,
    )
