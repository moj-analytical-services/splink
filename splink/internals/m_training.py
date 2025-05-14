import logging
from copy import deepcopy
from typing import TYPE_CHECKING

from splink.internals.blocking import BlockingRule, block_using_rules_sqls
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.expectation_maximisation import (
    compute_new_parameters_sql,
    compute_proportions_for_new_parameters,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import compute_df_concat_with_tf

from .m_u_records_to_parameters import (
    append_m_probability_to_comparison_level_trained_probabilities,
    m_u_records_to_lookup_dict,
)

if TYPE_CHECKING:
    from splink.internals.linker import Linker
logger = logging.getLogger(__name__)


def estimate_m_values_from_label_column(linker: "Linker", label_colname: str) -> None:
    msg = f" Estimating m probabilities using from column {label_colname} "
    logger.info(f"{msg:-^70}")

    original_settings_object = linker._settings_obj
    training_linker = deepcopy(linker)
    settings_obj = training_linker._settings_obj
    settings_obj._retain_matching_columns = False
    settings_obj._retain_intermediate_calculation_columns = False
    for cc in settings_obj.comparisons:
        for cl in cc.comparison_levels:
            # TODO: ComparisonLevel: manage access
            cl._tf_adjustment_column = None

    pipeline = CTEPipeline()
    nodes_with_tf = compute_df_concat_with_tf(linker, pipeline)

    pipeline = CTEPipeline([nodes_with_tf])

    sqls = block_using_rules_sqls(
        input_tablename_l="__splink__df_concat_with_tf",
        input_tablename_r="__splink__df_concat_with_tf",
        blocking_rules=[
            BlockingRule(
                f"l.{label_colname} = r.{label_colname}", linker._sql_dialect_str
            )
        ],
        link_type=settings_obj._link_type,
        source_dataset_input_column=settings_obj.column_info_settings.source_dataset_input_column,
        unique_id_input_column=settings_obj.column_info_settings.unique_id_input_column,
    )
    pipeline.enqueue_list_of_sqls(sqls)

    blocked_pairs = training_linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    pipeline = CTEPipeline([blocked_pairs, nodes_with_tf])

    sqls = compute_comparison_vector_values_from_id_pairs_sqls(
        training_linker._settings_obj._columns_to_select_for_blocking,
        training_linker._settings_obj._columns_to_select_for_comparison_vector_values,
        input_tablename_l="__splink__df_concat_with_tf",
        input_tablename_r="__splink__df_concat_with_tf",
        source_dataset_input_column=training_linker._settings_obj.column_info_settings.source_dataset_input_column,
        unique_id_input_column=training_linker._settings_obj.column_info_settings.unique_id_input_column,
    )

    pipeline.enqueue_list_of_sqls(sqls)

    sql = """
    select *, cast(1.0 as float8) as match_probability
    from __splink__df_comparison_vectors
    """
    pipeline.enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters_sql(
        estimate_without_term_frequencies=False,
        comparisons=settings_obj.comparisons,
    )
    pipeline.enqueue_sql(sql, "__splink__m_u_counts")

    df_params = training_linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    param_records = df_params.as_pandas_dataframe()
    param_records = compute_proportions_for_new_parameters(param_records)

    m_u_records = [
        r
        for r in param_records
        if r["output_column_name"] != "_probability_two_random_records_match"
    ]
    m_u_records_lookup = m_u_records_to_lookup_dict(m_u_records)
    for cc in original_settings_object.comparisons:
        for cl in cc._comparison_levels_excluding_null:
            append_m_probability_to_comparison_level_trained_probabilities(
                cl,
                m_u_records_lookup,
                cc.output_column_name,
                "estimate m from label column",
            )
