from copy import deepcopy
import logging

from .blocking import block_using_rules_sql
from .comparison_vector_values import compute_comparison_vector_values_sql
from .expectation_maximisation import compute_new_parameters
from .misc import m_u_records_to_lookup_dict

logger = logging.getLogger(__name__)


def estimate_m_values_from_label_column(linker, df_dict, label_colname):

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
    for cc in settings_obj.comparisons:
        for cl in cc.comparison_levels_excluding_null:

            try:
                m_probability = m_u_records_lookup[cc.comparison_name][
                    cl.comparison_vector_value
                ]["m_probability"]

            except KeyError:
                m_probability = ("no value found for this level in m_u_records",)

                logger.info(
                    f"m probability not trained for {cc.comparison_name} - "
                    f"{cl.label_for_charts} (comparison vector value: "
                    f"{cl.comparison_vector_value}). This usually means the "
                    "comparison level was never observed in the training data."
                )
            cl.add_trained_m_probability(
                m_probability,
                "estimate m from label column",
            )
