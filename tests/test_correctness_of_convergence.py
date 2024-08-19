# This is a test of whether Splink can recover the true parameters of a record
# linkage model using synthetic data generated using a known distribution
# As such it provides a good end-to-end test of whether our implementation of
# Fellegi Sunter is correct

# Data generated using
# github.com/moj-analytical-services/
# splink_data_generation/commit/7524ad6a7b7deadbb7b28813b625e1e6b14af95c
# settings_for_data_generation = {
#     "link_type": "dedupe_only",
#     "comparison_columns": [
#         {
#             "col_name": "col_1",
#             "m_probabilities": [0.3, 0.7],  # Probability of typo
#             "u_probabilities": [0.9, 0.1],  # Probability of collision
#         },
#         {
#             "col_name": "col_2",
#             "m_probabilities": [0.1, 0.9],  # Probability of typo
#             "u_probabilities": [0.975, 0.025],  # Probability of collision
#         },
#         {
#             "col_name": "col_3",
#             "m_probabilities": [0.05, 0.95],  # Probability of typo
#             "u_probabilities": [0.8, 0.2],  # Probability of collision
#         },
#     ],
# }

# df = generate_df_gammas_exact(settings_for_data_generation)
# df = add_match_prob(df, settings_for_data_generation)


import re

import pandas as pd
import pytest
from pandas.testing import assert_series_equal

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator
from splink.internals.duckdb.dataframe import DuckDBDataFrame
from splink.internals.em_training_session import EMTrainingSession
from splink.internals.exceptions import SplinkException
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import predict_from_comparison_vectors_sqls_using_settings


def test_splink_converges_to_known_params():
    df = pd.read_csv("./tests/datasets/known_params_comparison_vectors.csv")
    rec = [
        {
            "unique_id": 1,
            "col_1": "a",
            "col_2": "b",
            "col_3": "c",
            "true_match": 1,
        },
    ]
    in_df = pd.DataFrame(rec)

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("col_1"),
            cl.ExactMatch("col_2"),
            cl.ExactMatch("col_3"),
        ],
        max_iterations=200,
        em_convergence=0.00001,
        additional_columns_to_retain=["true_match", "true_match_probability"],
        retain_intermediate_calculation_columns=False,
        retain_matching_columns=False,
        linker_uid="abc",
    )

    db_api = DuckDBAPI()

    linker = Linker(in_df, settings, db_api=db_api)

    settings_obj = linker._settings_obj

    # We want to 'inject' the pre-computed comparison vectors into the linker

    em_training_session = EMTrainingSession(
        linker,
        db_api=db_api,
        blocking_rule_for_training="1=1",
        core_model_settings=settings_obj.core_model_settings,
        training_settings=settings_obj.training_settings,
        unique_id_input_columns=settings_obj.column_info_settings.unique_id_input_columns,
        fix_u_probabilities=False,
        fix_m_probabilities=False,
        fix_probability_two_random_records_match=False,
    )

    # This test is fiddly because you need to know the hash of the
    # comparison vector table 'in advance'.  To get it, we run
    # code that looks for the table and fails to find it
    # We can then register a table with that name
    try:
        em_training_session._comparison_vectors()
    except SplinkException as e:
        pattern = r"__splink__df_comparison_vectors_[a-f0-9]{9}"

        cvv_hashed_tablename = re.search(pattern, str(e)).group()

    cvv_table = db_api.register_table(df, cvv_hashed_tablename)
    cvv_table.templated_name = "__splink__df_comparison_vectors"

    core_model_settings = em_training_session._train(cvv_table)
    linker._settings_obj.core_model_settings = core_model_settings
    linker._em_training_sessions.append(em_training_session)

    linker._populate_m_u_from_trained_values()

    linker._populate_probability_two_random_records_match_from_trained_values()

    linker.visualisations.match_weights_chart()

    cv = DuckDBDataFrame(
        "__splink__df_comparison_vectors",
        cvv_hashed_tablename,
        linker,
    )

    pipeline = CTEPipeline([cv])
    sqls = predict_from_comparison_vectors_sqls_using_settings(
        linker._settings_obj,
        sql_infinity_expression=linker._infinity_expression,
    )
    pipeline.enqueue_list_of_sqls(sqls)

    predictions = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    predictions_df = predictions.as_pandas_dataframe()

    assert_series_equal(
        predictions_df["match_probability"],
        predictions_df["true_match_probability_l"],
        check_exact=False,
        rtol=0.01,
        check_names=False,
    )

    s_obj = linker._settings_obj
    assert s_obj._probability_two_random_records_match == pytest.approx(0.5, 0.01)

    param_dict = s_obj.comparisons[0].as_dict()
    cls = param_dict["comparison_levels"]
    assert cls[1]["m_probability"] == pytest.approx(0.7, abs=0.01)
    assert cls[1]["u_probability"] == pytest.approx(0.1, abs=0.01)
    assert cls[2]["m_probability"] == pytest.approx(0.3, abs=0.01)
    assert cls[2]["u_probability"] == pytest.approx(0.9, abs=0.01)
