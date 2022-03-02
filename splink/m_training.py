from copy import deepcopy

from .blocking import block_using_rules
from .comparison_vector_values import compute_comparison_vector_values
from .maximisation_step import compute_new_parameters


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

    sql = block_using_rules(training_linker)
    training_linker.enqueue_sql(sql, "__splink__df_blocked")

    sql = compute_comparison_vector_values(settings_obj)

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

    for record in m_u_records:
        cc = original_settings_object._get_comparison_by_name(record["comparison_name"])
        gamma_val = record["comparison_vector_value"]
        cl = cc.get_comparison_level_by_comparison_vector_value(gamma_val)

        cl.add_trained_m_probability(
            record["m_probability"], "estimate m from label column"
        )
