import logging
from .comparison_vector_values import compute_comparison_vector_values_sql
from .expectation_maximisation import compute_new_parameters
from .block_from_labels import block_from_labels
from .m_u_records_to_parameters import (
    m_u_records_to_lookup_dict,
    append_m_probability_to_comparison_level_trained_probabilities,
)


logger = logging.getLogger(__name__)


def estimate_m_from_pairwise_labels(linker, table_name):

    sqls = block_from_labels(linker, table_name)

    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])

    sql = compute_comparison_vector_values_sql(linker.settings_obj)

    linker.enqueue_sql(sql, "__splink__df_comparison_vectors")

    sql = """
    select *, 1.0D as match_probability
    from __splink__df_comparison_vectors
    """
    linker.enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters(linker.settings_obj)
    linker.enqueue_sql(sql, "__splink__df_new_params")

    df_params = linker.execute_sql_pipeline()

    param_records = df_params.as_record_dict()

    m_u_records = [
        r for r in param_records if r["comparison_name"] != "_proportion_of_matches"
    ]

    m_u_records_lookup = m_u_records_to_lookup_dict(m_u_records)
    for cc in linker.settings_obj.comparisons:
        for cl in cc.comparison_levels_excluding_null:
            append_m_probability_to_comparison_level_trained_probabilities(
                cl, m_u_records_lookup, "estimate m from pairwise labels"
            )

    linker.populate_m_u_from_trained_values()
