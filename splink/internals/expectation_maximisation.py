from __future__ import annotations

import logging
import time
from typing import Any, List, cast

import pandas as pd

from splink.internals.comparison import Comparison
from splink.internals.comparison_level import ComparisonLevel
from splink.internals.constants import LEVEL_NOT_OBSERVED_TEXT
from splink.internals.input_column import InputColumn
from splink.internals.m_u_records_to_parameters import m_u_records_to_lookup_dict
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import (
    predict_from_agreement_pattern_counts_sqls,
    predict_from_comparison_vectors_sqls,
)
from splink.internals.settings import CoreModelSettings, TrainingSettings
from splink.internals.splink_dataframe import SplinkDataFrame

from .database_api import DatabaseAPISubClass

logger = logging.getLogger(__name__)


def count_agreement_patterns_sql(comparisons: List[Comparison]) -> str:
    """Count how many times each realized agreement pattern
    was observed across the blocked dataset."""
    gamma_cols = [cc._gamma_column_name for cc in comparisons]
    gamma_cols_expr = ",".join(gamma_cols)

    sql = f"""
    select
    {gamma_cols_expr},
    count(*) as agreement_pattern_count
    from __splink__df_comparison_vectors
    group by {gamma_cols_expr}
    """

    return sql


def compute_new_parameters_sql(
    estimate_without_term_frequencies: bool, comparisons: List[Comparison]
) -> str:
    """compute m and u counts from the results of predict"""
    if estimate_without_term_frequencies:
        agreement_pattern_count = "agreement_pattern_count"
    else:
        agreement_pattern_count = "1"

    sql_template = """
    select
    {gamma_column} as comparison_vector_value,
    sum(match_probability * {agreement_pattern_count}) as m_count,
    sum((1-match_probability) * {agreement_pattern_count}) as u_count,
    '{output_column_name}' as output_column_name
    from __splink__df_predict
    group by {gamma_column}
    """
    union_sqls = [
        sql_template.format(
            gamma_column=cc._gamma_column_name,
            output_column_name=cc.output_column_name,
            agreement_pattern_count=agreement_pattern_count,
        )
        for cc in comparisons
    ]

    # Probability of two random records matching
    sql = f"""
    select 0 as comparison_vector_value,
           sum(match_probability * {agreement_pattern_count}) /
               sum({agreement_pattern_count}) as m_count,
           sum((1-match_probability) * {agreement_pattern_count}) /
               sum({agreement_pattern_count}) as u_count,
           '_probability_two_random_records_match' as output_column_name
    from __splink__df_predict
    """
    union_sqls.append(sql)

    sql = " union all ".join(union_sqls)

    return sql


def compute_proportions_for_new_parameters_sql(table_name):
    """Using the results from compute_new_parameters_sql, compute
    m and u
    """

    sql = f"""
    select
        comparison_vector_value,
        output_column_name,
        m_count/sum(m_count) over (PARTITION BY output_column_name)
            as m_probability,
        u_count/sum(u_count) over (PARTITION BY output_column_name)
            as u_probability
    from {table_name}
    where comparison_vector_value != -1
    and output_column_name != '_probability_two_random_records_match'

    union all

    select
        comparison_vector_value,
        output_column_name,
        m_count as m_probability,
        u_count as u_probability
    from {table_name}
    where output_column_name = '_probability_two_random_records_match'
    order by output_column_name, comparison_vector_value asc
    """

    return sql


def compute_proportions_for_new_parameters_pandas(
    m_u_df: pd.DataFrame,
) -> List[dict[str, Any]]:
    data = m_u_df.copy()
    m_prob = "m_probability"
    u_prob = "u_probability"
    data.rename(columns={"m_count": m_prob, "u_count": u_prob}, inplace=True)

    random_records = data[
        data.output_column_name == "_probability_two_random_records_match"
    ]
    data = data[data.output_column_name != "_probability_two_random_records_match"]

    data = data[data.comparison_vector_value != -1]
    index = data.index.tolist()

    m_probs = data.loc[index, m_prob] / data.groupby("output_column_name")[
        m_prob
    ].transform("sum")
    u_probs = data.loc[index, u_prob] / data.groupby("output_column_name")[
        u_prob
    ].transform("sum")

    data.loc[index, m_prob] = m_probs
    data.loc[index, u_prob] = u_probs

    data = pd.concat([random_records, data])

    return data.to_dict("records")


def compute_proportions_for_new_parameters(
    m_u_df: pd.DataFrame,
) -> List[dict[str, Any]]:
    # Execute with duckdb if installed, otherwise default to pandas
    try:
        import duckdb

        sql = compute_proportions_for_new_parameters_sql("m_u_df")
        return duckdb.query(sql).to_df().to_dict("records")
    except (ImportError, ModuleNotFoundError):
        return compute_proportions_for_new_parameters_pandas(m_u_df)


def populate_m_u_from_lookup(
    training_fixed_probabilities: set[str],
    comparison_level: ComparisonLevel,
    output_column_name: str,
    m_u_records_lookup: dict[str, dict[int, Any]],
) -> None:
    cl = comparison_level

    if not cl._fix_m_probability and "m" not in training_fixed_probabilities:
        try:
            m_probability = m_u_records_lookup[output_column_name][
                cl.comparison_vector_value
            ]["m_probability"]

        except KeyError:
            m_probability = LEVEL_NOT_OBSERVED_TEXT
            cc_n = output_column_name
            cl_n = cl.label_for_charts
            if not cl._m_warning_sent:
                logger.warning(
                    "WARNING:\n"
                    f"Level {cl_n} on comparison {cc_n} not observed in dataset, "
                    "unable to train m value\n"
                )
                cl._m_warning_sent = True
        cl.m_probability = m_probability

    if not cl._fix_u_probability and "u" not in training_fixed_probabilities:
        try:
            u_probability = m_u_records_lookup[output_column_name][
                cl.comparison_vector_value
            ]["u_probability"]

        except KeyError:
            u_probability = LEVEL_NOT_OBSERVED_TEXT

            cc_n = output_column_name
            cl_n = cl.label_for_charts
            if not cl._u_warning_sent:
                logger.warning(
                    "WARNING:\n"
                    f"Level {cl_n} on comparison {cc_n} not observed in dataset, "
                    "unable to train u value\n"
                )
                cl._u_warning_sent = True

        cl.u_probability = u_probability


def maximisation_step(
    training_fixed_probabilities: set[str],
    core_model_settings: CoreModelSettings,
    param_records: List[dict[str, Any]],
) -> CoreModelSettings:
    core_model_settings = core_model_settings.copy()

    m_u_records = []
    for r in param_records:
        if r["output_column_name"] == "_probability_two_random_records_match":
            prop_record = r
        else:
            m_u_records.append(r)

    if "lambda" not in training_fixed_probabilities:
        core_model_settings.probability_two_random_records_match = prop_record[
            "m_probability"
        ]

    m_u_records_lookup = m_u_records_to_lookup_dict(m_u_records)
    for cc in core_model_settings.comparisons:
        for cl in cc._comparison_levels_excluding_null:
            populate_m_u_from_lookup(
                training_fixed_probabilities,
                cl,
                cc.output_column_name,
                m_u_records_lookup,
            )

    return core_model_settings


def expectation_maximisation(
    db_api: DatabaseAPISubClass,
    training_settings: TrainingSettings,
    estimate_without_term_frequencies: bool,
    core_model_settings: CoreModelSettings,
    unique_id_input_columns: List[InputColumn],
    training_fixed_probabilities: set[str],
    df_comparison_vector_values: SplinkDataFrame,
) -> List[CoreModelSettings]:
    """In the expectation step, we use the current model parameters to estimate
    the probability of match for each pairwise record comparison

    In the maximisation step, we use these predicted probabilities to re-compute
    the parameters of the model
    """
    # initial values of parameters
    core_model_settings_history = [core_model_settings.copy()]

    sql_infinity_expression = db_api.sql_dialect.infinity_expression

    max_iterations = training_settings.max_iterations
    em_convergence = training_settings.em_convergence
    logger.info("")  # newline

    if estimate_without_term_frequencies:
        sql = count_agreement_patterns_sql(core_model_settings.comparisons)
        pipeline = CTEPipeline([df_comparison_vector_values])
        pipeline.enqueue_sql(sql, "__splink__agreement_pattern_counts")
        agreement_pattern_counts = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    for i in range(1, max_iterations + 1):
        pipeline = CTEPipeline()
        probability_two_random_records_match = (
            core_model_settings.probability_two_random_records_match
        )
        start_time = time.time()

        # Expectation step
        if estimate_without_term_frequencies:
            sqls = predict_from_agreement_pattern_counts_sqls(
                core_model_settings.comparisons,
                probability_two_random_records_match,
                sql_dialect=db_api.sql_dialect,
                sql_infinity_expression=db_api.sql_dialect.infinity_expression,
            )
        else:
            sqls = predict_from_comparison_vectors_sqls(
                unique_id_input_columns=unique_id_input_columns,
                core_model_settings=core_model_settings,
                sql_dialect=db_api.sql_dialect,
                training_mode=True,
                sql_infinity_expression=sql_infinity_expression,
            )

        for sql_info in sqls:
            pipeline.enqueue_sql(sql_info["sql"], sql_info["output_table_name"])

        sql = compute_new_parameters_sql(
            estimate_without_term_frequencies,
            core_model_settings.comparisons,
        )
        pipeline.enqueue_sql(sql, "__splink__m_u_counts")
        if estimate_without_term_frequencies:
            pipeline.append_input_dataframe(agreement_pattern_counts)
            df_params = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        else:
            pipeline.append_input_dataframe(df_comparison_vector_values)
            df_params = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        param_records = df_params.as_pandas_dataframe()
        param_records = compute_proportions_for_new_parameters(param_records)

        df_params.drop_table_from_database_and_remove_from_cache()

        core_model_settings = maximisation_step(
            training_fixed_probabilities=training_fixed_probabilities,
            core_model_settings=core_model_settings,
            param_records=param_records,
        )
        core_model_settings_history.append(core_model_settings)

        max_change_dict = _max_change_in_parameters_comparison_levels(
            core_model_settings_history
        )
        logger.info(f"Iteration {i}: {max_change_dict['message']}")
        end_time = time.time()
        logger.log(15, f"    Iteration time: {end_time - start_time} seconds")

        if max_change_dict["max_abs_change_value"] < em_convergence:
            break

    logger.info(f"\nEM converged after {i} iterations")
    return core_model_settings_history


def _max_change_message(max_change_dict):
    message = "Largest change in params was"

    if max_change_dict["max_change_type"] == "probability_two_random_records_match":
        message = (
            f"{message} {max_change_dict['max_change_value']:,.3g} in "
            "probability_two_random_records_match"
        )
    else:
        cl = max_change_dict["current_comparison_level"]
        m_u = max_change_dict["max_change_type"]
        cc_name = max_change_dict["output_column_name"]

        cl_label = cl.label_for_charts
        level_text = f"{cc_name}, level `{cl_label}`"

        message = (
            f"{message} {max_change_dict['max_change_value']:,.3g} in "
            f"the {m_u} of {level_text}"
        )

    return message


def _max_change_in_parameters_comparison_levels(
    core_model_settings_history: List[CoreModelSettings],
) -> dict[str, Any]:
    previous_iteration = core_model_settings_history[-2]
    this_iteration = core_model_settings_history[-1]
    max_change = -0.1

    max_change_levels: dict[str, Any] = {
        "previous_iteration": None,
        "this_iteration": None,
        "max_change_type": None,
        "max_change_value": None,
    }
    comparisons = zip(previous_iteration.comparisons, this_iteration.comparisons)
    for comparison in comparisons:
        prev_cc = comparison[0]
        this_cc = comparison[1]
        z_cls = zip(prev_cc.comparison_levels, this_cc.comparison_levels)
        for z_cl in z_cls:
            if z_cl[0].is_null_level:
                continue
            prev_cl = z_cl[0]
            this_cl = z_cl[1]

            prev_m_prob = cast(float, prev_cl.m_probability)
            this_m_prob = cast(float, this_cl.m_probability)
            prev_u_prob = cast(float, prev_cl.u_probability)
            this_u_prob = cast(float, this_cl.u_probability)
            change_m = this_m_prob - prev_m_prob
            change_u = this_u_prob - prev_u_prob

            change = max(abs(change_m), abs(change_u))
            change_type = (
                "m_probability" if abs(change_m) > abs(change_u) else "u_probability"
            )
            change_value = change_m if abs(change_m) > abs(change_u) else change_u
            if change > max_change:
                max_change = change
                max_change_levels["prev_comparison_level"] = prev_cl
                max_change_levels["current_comparison_level"] = this_cl
                max_change_levels["max_change_type"] = change_type
                max_change_levels["max_change_value"] = change_value
                max_change_levels["max_abs_change_value"] = abs(change_value)
                max_change_levels["output_column_name"] = this_cc.output_column_name

    change_probability_two_random_records_match = (
        this_iteration.probability_two_random_records_match
        - previous_iteration.probability_two_random_records_match
    )

    if abs(change_probability_two_random_records_match) > max_change:
        max_change = abs(change_probability_two_random_records_match)
        max_change_levels["prev_comparison_level"] = None
        max_change_levels["current_comparison_level"] = None
        max_change_levels["max_change_type"] = "probability_two_random_records_match"
        max_change_levels["max_change_value"] = (
            change_probability_two_random_records_match
        )
        max_change_levels["max_abs_change_value"] = abs(
            change_probability_two_random_records_match
        )

    max_change_levels["message"] = _max_change_message(max_change_levels)

    return max_change_levels
