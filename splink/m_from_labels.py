import logging
from .comparison_vector_values import compute_comparison_vector_values_sql
from .expectation_maximisation import (
    compute_new_parameters_sql,
    compute_proportions_for_new_parameters,
)
from .block_from_labels import block_from_labels
from .m_u_records_to_parameters import (
    m_u_records_to_lookup_dict,
    append_m_probability_to_comparison_level_trained_probabilities,
)


logger = logging.getLogger(__name__)


def estimate_m_from_pairwise_labels(linker, table_name):

    sqls = block_from_labels(linker, table_name)

    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    sql = compute_comparison_vector_values_sql(linker._settings_obj)

    linker._enqueue_sql(sql, "__splink__df_comparison_vectors")

    sql = """
    select *, cast(1.0 as double) as match_probability
    from __splink__df_comparison_vectors
    """
    linker._enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters_sql(linker._settings_obj)
    linker._enqueue_sql(sql, "__splink__m_u_counts")

    df_params = linker._execute_sql_pipeline()
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
            append_m_probability_to_comparison_level_trained_probabilities(
                cl, m_u_records_lookup, "estimate m from pairwise labels"
            )

    linker._populate_m_u_from_trained_values()
