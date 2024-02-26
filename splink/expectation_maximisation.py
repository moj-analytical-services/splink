from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, List

import pandas as pd

from .comparison import Comparison
from .comparison_level import ComparisonLevel
from .constants import LEVEL_NOT_OBSERVED_TEXT
from .m_u_records_to_parameters import m_u_records_to_lookup_dict
from .pipeline import SQLPipeline
from .predict import (
    predict_from_agreement_pattern_counts_sqls,
    predict_from_comparison_vectors_sqls,
)
from .settings import CoreModelSettings
from .splink_dataframe import SplinkDataFrame

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .em_training_session import EMTrainingSession


logger = logging.getLogger(__name__)


def count_agreement_patterns_sql(comparisons: List[Comparison]):
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
):
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


def compute_proportions_for_new_parameters_pandas(m_u_df: pd.DataFrame) -> List[dict]:
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


def compute_proportions_for_new_parameters(m_u_df: pd.DataFrame) -> List[dict]:
    # Execute with duckdb if installed, otherwise default to pandas
    try:
        import duckdb

        sql = compute_proportions_for_new_parameters_sql("m_u_df")
        return duckdb.query(sql).to_df().to_dict("records")
    except (ImportError, ModuleNotFoundError):
        return compute_proportions_for_new_parameters_pandas(m_u_df)


def populate_m_u_from_lookup(
    fix_m_probabilities: bool,
    fix_u_probabilities: bool,
    comparison_level: ComparisonLevel,
    output_column_name: str,
    m_u_records_lookup,
) -> None:
    cl = comparison_level

    if not fix_m_probabilities:
        try:
            m_probability = m_u_records_lookup[output_column_name][
                cl._comparison_vector_value
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

    if not fix_u_probabilities:
        try:
            u_probability = m_u_records_lookup[output_column_name][
                cl._comparison_vector_value
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
    fix_m_probabilities: bool,
    fix_u_probabilities: bool,
    fix_probability_two_random_records_match: bool,
    core_model_settings: CoreModelSettings,
    param_records: List[dict],
) -> CoreModelSettings:
    # settings_obj = em_training_session._settings_obj
    core_model_settings = core_model_settings.copy()

    m_u_records = []
    for r in param_records:
        if r["output_column_name"] == "_probability_two_random_records_match":
            prop_record = r
        else:
            m_u_records.append(r)

    if not fix_probability_two_random_records_match:
        core_model_settings.probability_two_random_records_match = prop_record[
            "m_probability"
        ]

    m_u_records_lookup = m_u_records_to_lookup_dict(m_u_records)
    for cc in core_model_settings.comparisons:
        for cl in cc._comparison_levels_excluding_null:
            populate_m_u_from_lookup(
                fix_m_probabilities,
                fix_u_probabilities,
                cl,
                cc.output_column_name,
                m_u_records_lookup,
            )

    return core_model_settings


def expectation_maximisation(
    em_training_session: EMTrainingSession,
    df_comparison_vector_values: SplinkDataFrame,
):
    """In the expectation step, we use the current model parameters to estimate
    the probability of match for each pairwise record comparison

    In the maximisation step, we use these predicted probabilities to re-compute
    the parameters of the model
    """

    settings_obj = em_training_session._settings_obj
    training_settings = settings_obj.training_settings
    core_model_settings = settings_obj.core_model_settings
    # original_core_model_settings = core_model_settings
    # comparisons = core_model_settings.comparisons

    db_api = em_training_session.db_api

    fix_m_probabilities = em_training_session._training_fix_m_probabilities
    fix_u_probabilities = em_training_session._training_fix_u_probabilities
    fix_probability_two_random_records_match = (
        em_training_session._training_fix_probability_two_random_records_match
    )

    max_iterations = training_settings.max_iterations
    em_convergence = training_settings.em_convergence
    logger.info("")  # newline

    # pipeline to execute the SQL we need to
    pipeline = SQLPipeline()

    if training_settings.estimate_without_term_frequencies:
        sql = count_agreement_patterns_sql(core_model_settings.comparisons)
        pipeline.enqueue_sql(sql, "__splink__agreement_pattern_counts")
        agreement_pattern_counts = db_api._execute_sql_pipeline(
            pipeline=pipeline, input_dataframes=[df_comparison_vector_values]
        )

    for i in range(1, max_iterations + 1):
        probability_two_random_records_match = (
            core_model_settings.probability_two_random_records_match
        )
        start_time = time.time()

        # Expectation step
        if training_settings.estimate_without_term_frequencies:
            sqls = predict_from_agreement_pattern_counts_sqls(
                core_model_settings.comparisons,
                probability_two_random_records_match,
                sql_infinity_expression=db_api.sql_dialect.infinity_expression,
            )
        else:
            # TODO: only need core model stuff - don't care here about extra cols / retains that are user-set!
            sqls = predict_from_comparison_vectors_sqls(
                settings_obj,
                sql_infinity_expression=db_api.sql_dialect.infinity_expression,
            )

        for sql in sqls:
            pipeline.enqueue_sql(sql["sql"], sql["output_table_name"])

        sql = compute_new_parameters_sql(
            training_settings.estimate_without_term_frequencies,
            core_model_settings.comparisons,
        )
        pipeline.enqueue_sql(sql, "__splink__m_u_counts")
        if training_settings.estimate_without_term_frequencies:
            df_params = db_api._execute_sql_pipeline(
                pipeline, [agreement_pattern_counts]
            )
        else:
            df_params = db_api._execute_sql_pipeline(
                pipeline, [df_comparison_vector_values]
            )
        param_records = df_params.as_pandas_dataframe()
        param_records = compute_proportions_for_new_parameters(param_records)

        df_params.drop_table_from_database_and_remove_from_cache()

        core_model_settings = maximisation_step(
            fix_m_probabilities=fix_m_probabilities,
            fix_u_probabilities=fix_u_probabilities,
            fix_probability_two_random_records_match=fix_probability_two_random_records_match,
            core_model_settings=core_model_settings,
            param_records=param_records,
        )
        # store a copy of current params on em_training_session
        em_training_session._add_iteration(core_model_settings)
        # TODO: remove this - update settings and then return:
        settings_obj.core_model_settings = core_model_settings

        max_change_dict = (
            em_training_session._max_change_in_parameters_comparison_levels()
        )
        logger.info(f"Iteration {i}: {max_change_dict['message']}")
        end_time = time.time()
        logger.log(15, f"    Iteration time: {end_time - start_time} seconds")

        if max_change_dict["max_abs_change_value"] < em_convergence:
            break

    logger.info(f"\nEM converged after {i} iterations")
    return core_model_settings
