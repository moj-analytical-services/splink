from copy import deepcopy
import logging

from .blocking import BlockingRule, block_using_rules_sql
from .comparison_vector_values import compute_comparison_vector_values_sql
from .expectation_maximisation import (
    compute_new_parameters_sql,
    compute_proportions_for_new_parameters,
)
from .m_u_records_to_parameters import (
    m_u_records_to_lookup_dict,
    append_m_probability_to_comparison_level_trained_probabilities,
)

logger = logging.getLogger(__name__)


def estimate_m_values_from_label_column(linker, df_dict, label_colname):

    msg = f" Estimating m probabilities using from column {label_colname} "
    logger.info(f"{msg:-^70}")

    original_settings_object = linker._settings_obj
    training_linker = deepcopy(linker)
    settings_obj = training_linker._settings_obj
    settings_obj._retain_matching_columns = False
    settings_obj._retain_intermediate_calculation_columns = False
    for cc in settings_obj.comparisons:
        for cl in cc.comparison_levels:
            cl._level_dict["tf_adjustment_column"] = None

    settings_obj._blocking_rules_to_generate_predictions = [
        BlockingRule(f"l.{label_colname} = r.{label_colname}")
    ]

    sql = block_using_rules_sql(training_linker)
    training_linker._enqueue_sql(sql, "__splink__df_blocked")

    sql = compute_comparison_vector_values_sql(settings_obj)

    training_linker._enqueue_sql(sql, "__splink__df_comparison_vectors")

    sql = """
    select *, cast(1.0 as double) as match_probability
    from __splink__df_comparison_vectors
    """
    training_linker._enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters_sql(settings_obj)
    training_linker._enqueue_sql(sql, "__splink__m_u_counts")

    df_params = training_linker._execute_sql_pipeline()
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
                cl, m_u_records_lookup, "estimate m from label column"
            )
