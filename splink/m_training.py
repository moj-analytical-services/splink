from copy import deepcopy
import logging

from .blocking import block_using_rules_sql
from .comparison_vector_values import compute_comparison_vector_values_sql
from .expectation_maximisation import compute_new_parameters
from .m_u_records_to_parameters import (
    m_u_records_to_lookup_dict,
    append_m_probability_to_comparison_level_trained_probabilities,
)

logger = logging.getLogger(__name__)


def estimate_m_values_from_label_column(linker, df_dict, label_colname):

    original_settings_object = linker.settings_obj
    training_linker = deepcopy(linker)
    settings_obj = training_linker.settings_obj
    settings_obj._retain_matching_columns = False
    settings_obj._retain_intermediate_calculation_columns = False
    for cc in settings_obj.comparisons:
        for cl in cc.comparison_levels:
            cl._level_dict["tf_adjustment_column"] = None

    settings_obj._blocking_rules_to_generate_predictions = [
        f"l.{label_colname} = r.{label_colname}"
    ]

    sql = block_using_rules_sql(training_linker)
    training_linker.enqueue_sql(sql, "__splink__df_blocked")

    sql = compute_comparison_vector_values_sql(settings_obj)

    training_linker.enqueue_sql(sql, "__splink__df_comparison_vectors")

    sql = """
    select *, 1.0D as match_probability
    from __splink__df_comparison_vectors
    """
    training_linker.enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters(settings_obj)
    training_linker.enqueue_sql(sql, "__splink__df_new_params")

    df_params = training_linker.execute_sql_pipeline()
    param_records = df_params.as_record_dict()

    m_u_records = [
        r for r in param_records if r["comparison_name"] != "_proportion_of_matches"
    ]
    m_u_records_lookup = m_u_records_to_lookup_dict(m_u_records)
    for cc in original_settings_object.comparisons:
        for cl in cc.comparison_levels_excluding_null:
            append_m_probability_to_comparison_level_trained_probabilities(
                cl, m_u_records_lookup, "estimate m from label column"
            )
