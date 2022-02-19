from copy import deepcopy

from .blocking import block_using_rules
from .comparison_vector_values import compute_comparison_vector_values
from .maximisation_step import _compute_new_parameters


def estimate_m_values_from_label_column(linker, sql_pipeline, label_colname):

    original_settings_object = linker.settings_obj
    settings_obj = deepcopy(linker.settings_obj)
    settings_obj._retain_matching_columns = False
    settings_obj._retain_intermediate_calculation_columns = False
    for cc in settings_obj.comparisons:
        for cl in cc.comparison_levels:
            cl._level_dict["tf_adjustment_column"] = None

    settings_obj._blocking_rules_to_generate_predictions = [
        f"l.{label_colname} = r.{label_colname}"
    ]

    sql_pipeline = block_using_rules(settings_obj, sql_pipeline, linker.execute_sql)

    sql_pipeline = compute_comparison_vector_values(
        settings_obj, sql_pipeline, linker.execute_sql
    )

    sql = """
    select *, 1.0D as match_probability
    from __splink__df_comparison_vectors
    """
    sql_pipeline = linker.execute_sql(sql, sql_pipeline, "__splink__df_predict")

    sql_pipeline = _compute_new_parameters(settings_obj, sql_pipeline, linker.execute_sql)
    param_records = sql_pipeline["__splink__df_new_params"].as_record_dict()

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
