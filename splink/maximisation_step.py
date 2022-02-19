import logging

from .predict import predict
from .settings import Settings

logger = logging.getLogger(__name__)


def _compute_new_parameters(settings_obj: Settings, sql_pipeline, generate_sql):
    """compute m and u from results of predict"""

    sql_template = """
    select {gamma_column} as comparison_vector_value,
           sum(match_probability)/(select sum(match_probability) from __splink__df_predict where {gamma_column} != -1) as m_probability,
           sum(1 - match_probability)/(select sum(1 - match_probability) from __splink__df_predict where {gamma_column} != -1) as u_probability,
           "{comparison_name}" as comparison_name
    from __splink__df_predict
    where {gamma_column} != -1
    group by {gamma_column}
    """
    sqls = [
        sql_template.format(
            gamma_column=cc.gamma_column_name,
            comparison_name=cc.comparison_name,
        )
        for cc in settings_obj.comparisons
    ]

    # Proportion of matches
    sql = """
    select 0 as comparison_vector_value,
           avg(match_probability) as m_probability,
           avg(1-match_probability) as u_probability,
           "_proportion_of_matches" as comparison_name
    from __splink__df_predict
    """
    sqls.append(sql)

    sql = " union all ".join(sqls)

    return generate_sql(sql, sql_pipeline, "__splink__df_new_params")


# SQL piping method if we need to revert back
# def maximisation_step(
#     em_training_session,
#     sql_pipeline,
#     generate_sql,
#     execute_sql
# ):

#     import time
#     t = time.time()

#     settings_obj = em_training_session.settings_obj
#     _compute_new_parameters(settings_obj, sql_pipeline, generate_sql)
#     print("--- _compute_new_parameters... %s seconds ---" % (time.time() - t))
#     t = time.time()
#     param_records = execute_sql(sql_pipeline).to_dict(orient="records")
#     print("--- Generate param_records... %s seconds ---" % (time.time() - t))

#     m_u_records = []
#     for r in param_records:
#         if r["comparison_name"] == "_proportion_of_matches":
#             prop_record = r
#         else:
#             m_u_records.append(r)

#     if not em_training_session._training_fix_proportion_of_matches:

#         settings_obj._proportion_of_matches = prop_record["m_probability"]

#     for record in m_u_records:
#         cc = settings_obj._get_comparison_by_name(record["comparison_name"])
#         gamma_val = record["comparison_vector_value"]
#         cl = cc.get_comparison_level_by_comparison_vector_value(gamma_val)

#         if not em_training_session._training_fix_m_probabilities:
#             cl.m_probability = record["m_probability"]

#         if not em_training_session._training_fix_u_probabilities:
#             cl.u_probability = record["u_probability"]

#     # Dump current comparsion columns to training settion
#     em_training_session.add_iteration()

#     return sql_pipeline

def maximisation_step(
    em_training_session,
    df_dict,
    execute_sql,
):
    import time
    t = time.time()

    settings_obj = em_training_session.settings_obj
    df_dict = _compute_new_parameters(settings_obj, df_dict, execute_sql)
    print("--- _compute_new_parameters... %s seconds ---" % (time.time() - t))
    t = time.time()
    param_records = df_dict["__splink__df_new_params"].as_record_dict()

    print("--- Generate param_records... %s seconds ---" % (time.time() - t))

    m_u_records = []
    for r in param_records:
        if r["comparison_name"] == "_proportion_of_matches":
            prop_record = r
        else:
            m_u_records.append(r)

    if not em_training_session._training_fix_proportion_of_matches:

        settings_obj._proportion_of_matches = prop_record["m_probability"]

    for record in m_u_records:
        cc = settings_obj._get_comparison_by_name(record["comparison_name"])
        gamma_val = record["comparison_vector_value"]
        cl = cc.get_comparison_level_by_comparison_vector_value(gamma_val)

        if not em_training_session._training_fix_m_probabilities:
            cl.m_probability = record["m_probability"]

        if not em_training_session._training_fix_u_probabilities:
            cl.u_probability = record["u_probability"]

    # Dump current comparsion columns to training settion
    em_training_session.add_iteration()

    return df_dict


# def expectation_maximisation(em_training_session, sql_pipeline, generate_sql, execute_sql):

#     settings_obj = em_training_session.settings_obj

#     max_iterations = settings_obj._max_iterations
#     em_convergece = settings_obj._em_convergence
#     for i in range(max_iterations):
#         sql_pipe = sql_pipeline.copy()
#         sql_pipe = predict(settings_obj, sql_pipe, generate_sql)
#         maximisation_step(
#             em_training_session,
#             sql_pipe,
#             generate_sql,
#             execute_sql,
#         )
#         max_change_dict = (
#             em_training_session.max_change_in_parameters_comparison_levels()
#         )
#         print(f"Iteration {i}: {max_change_dict['message']}")

#         if max_change_dict["max_abs_change_value"] < em_convergece:
#             break
#     print(f"EM converged after {i} iterations")

def expectation_maximisation(em_training_session, df_dict, execute_sql):

    settings_obj = em_training_session.settings_obj

    max_iterations = settings_obj._max_iterations
    em_convergece = settings_obj._em_convergence
    for i in range(max_iterations):
        df_dict = predict(settings_obj, df_dict, execute_sql)
        maximisation_step(
            em_training_session,
            df_dict,
            execute_sql,
        )
        max_change_dict = (
            em_training_session.max_change_in_parameters_comparison_levels()
        )
        print(f"Iteration {i}: {max_change_dict['message']}")

        if max_change_dict["max_abs_change_value"] < em_convergece:
            break
    print(f"EM converged after {i} iterations")
