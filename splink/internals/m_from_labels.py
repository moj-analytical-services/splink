import logging
from typing import TYPE_CHECKING

from splink.internals.block_from_labels import block_from_labels
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_sql,
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


def estimate_m_from_pairwise_labels(linker: "Linker", table_name: str) -> None:
    pipeline = CTEPipeline()
    nodes_with_tf = compute_df_concat_with_tf(linker, pipeline)
    pipeline = CTEPipeline([nodes_with_tf])
    sqls = block_from_labels(linker, table_name)

    pipeline.enqueue_list_of_sqls(sqls)

    sql = compute_comparison_vector_values_sql(
        linker._settings_obj._columns_to_select_for_comparison_vector_values
    )

    pipeline.enqueue_sql(sql, "__splink__df_comparison_vectors")

    sql = """
    select *, cast(1.0 as float8) as match_probability
    from __splink__df_comparison_vectors
    """
    pipeline.enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters_sql(
        estimate_without_term_frequencies=False,
        comparisons=linker._settings_obj.comparisons,
    )
    pipeline.enqueue_sql(sql, "__splink__m_u_counts")

    df_params = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    param_records = df_params.as_pandas_dataframe()
    param_records = compute_proportions_for_new_parameters(param_records)

    m_u_records = [
        r
        for r in param_records
        if r["output_column_name"] != "_probability_two_random_records_match"
    ]

    m_u_records_lookup = m_u_records_to_lookup_dict(m_u_records)
    for cc in linker._settings_obj.comparisons:
        for cl in cc._comparison_levels_excluding_null:
            if not cl._fix_m_probability:
                append_m_probability_to_comparison_level_trained_probabilities(
                    cl,
                    m_u_records_lookup,
                    cc.output_column_name,
                    "estimate m from pairwise labels",
                )

    linker._populate_m_u_from_trained_values()
