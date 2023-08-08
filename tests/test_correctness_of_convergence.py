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


import pandas as pd
import pytest

import splink.duckdb.comparison_library as cl
from splink.duckdb.linker import DuckDBDataFrame, DuckDBLinker
from splink.em_training_session import EMTrainingSession
from splink.predict import predict_from_comparison_vectors_sqls


def test_splink_converges_to_known_params():
    df = pd.read_csv("./tests/datasets/known_params_comparison_vectors.csv")
    df.head()

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.exact_match("col_1"),
            cl.exact_match("col_2"),
            cl.exact_match("col_3"),
        ],
        "max_iterations": 200,
        "em_convergence": 0.00001,
        "additional_columns_to_retain": ["true_match", "true_match_probability"],
        "retain_intermediate_calculation_columns": False,
        "retain_matching_columns": False,
        "linker_uid": "abc",
    }

    linker = DuckDBLinker(df, settings)

    # This test is fiddly because you need to know the hash of the
    # comparison vector table, but to find this out you need to run the test

    # If the test is failing, run it and look at the output for a line like
    # CREATE TABLE __splink__df_comparison_vectors_abc123
    # and modify the following line to include the value of the hash (abc123 above)

    cvv_hashed_tablename = "__splink__df_comparison_vectors_ee08ffa85"
    linker.register_table(df, cvv_hashed_tablename)

    em_training_session = EMTrainingSession(
        linker,
        "1=1",
        fix_u_probabilities=False,
        fix_m_probabilities=False,
        fix_probability_two_random_records_match=False,
    )

    em_training_session._train()

    linker._populate_m_u_from_trained_values()

    linker._populate_probability_two_random_records_match_from_trained_values()

    linker.match_weights_chart()

    cv = DuckDBDataFrame(
        "__splink__df_comparison_vectors",
        cvv_hashed_tablename,
        linker,
    )

    sqls = predict_from_comparison_vectors_sqls(
        linker._settings_obj,
        sql_infinity_expression=linker._infinity_expression,
    )

    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    predictions = linker._execute_sql_pipeline([cv])
    predictions_df = predictions.as_pandas_dataframe()

    from pandas.testing import assert_series_equal

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
