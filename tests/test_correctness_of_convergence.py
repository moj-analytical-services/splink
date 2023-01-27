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

import splink.duckdb.duckdb_comparison_library as cl
from splink.duckdb.duckdb_linker import DuckDBLinker, DuckDBLinkerDataFrame
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
    }

    linker = DuckDBLinker(df, settings)

    # need to register df_blocked and go from there
    linker.register_table(df, "__splink__df_comparison_vectors_0de5e3a")

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

    cv = DuckDBLinkerDataFrame(
        "__splink__df_comparison_vectors",
        "__splink__df_comparison_vectors_0de5e3a",
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
    param_dict["comparison_levels"][1]["m_probability"] == pytest.approx(0.7, abs=0.01)
    param_dict["comparison_levels"][1]["u_probability"] == pytest.approx(0.1, abs=0.01)
    param_dict["comparison_levels"][2]["m_probability"] == pytest.approx(0.3, abs=0.01)
    param_dict["comparison_levels"][2]["u_probability"] == pytest.approx(0.9, abs=0.01)
