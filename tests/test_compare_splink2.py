import pytest

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
from splink.internals.misc import bayes_factor_to_prob, prob_to_bayes_factor

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_splink_2_predict(fake_1000, dialect, test_helpers):
    helper = test_helpers[dialect]
    settings_dict = get_settings_dict()
    db_api = helper.db_api()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings_dict)

    expected_sdf = db_api.register_from_csv("tests/datasets/splink2_479_vs_481.csv")

    df_e = linker.inference.predict()

    actual_record = df_e.query_sql(
        "SELECT * FROM {this} WHERE unique_id_l = 479 AND unique_id_r = 481"
    )

    expected_match_weight = float(expected_sdf.as_dict()["match_weight"][0])
    actual_match_weight = actual_record.as_dict()["match_weight"][0]

    assert actual_match_weight == pytest.approx(expected_match_weight)


def test_splink_2_em_fixed_u(fake_1000, test_helpers):
    settings_dict = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings_dict)

    # Check lambda history is the same
    expected_prop_history = db_api.register_from_csv(
        "tests/datasets/splink2_proportion_of_matches_history_fixed_u.csv"
    )

    training_session = (
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.surname = r.surname"
        )
    )
    actual_prop_history = db_api.register(training_session._lambda_history_records)
    actuals = sorted(actual_prop_history.as_record_dict(), key=lambda r: r["iteration"])
    expecteds = sorted(
        expected_prop_history.as_record_dict(), key=lambda r: r["iteration"]
    )

    for expected, actual in zip(expecteds, actuals):
        assert expected["λ"] == pytest.approx(
            actual["probability_two_random_records_match"]
        )

    # Check history of m probabilities is the same for a column
    expected_m_u_history = db_api.register_from_csv(
        "tests/datasets/splink2_m_u_history_fixed_u.csv"
    )
    expected_first_name_level_1_m = expected_m_u_history.query_sql(
        """
            SELECT *
            FROM {this}
            WHERE gamma_column_name = 'gamma_first_name'
            AND comparison_vector_value = 1
        """
    )

    actual_m_u_history = db_api.register(
        list(map(lambda r: r.as_dict(), training_session._iteration_history_records))
    )
    actual_first_name_level_1_m = actual_m_u_history.query_sql(
        """
            SELECT *
            FROM {this}
            WHERE comparison_name = 'first_name'
            AND comparison_vector_value = 1
        """
    )

    actuals = sorted(
        actual_first_name_level_1_m.as_record_dict(), key=lambda r: r["iteration"]
    )
    expecteds = sorted(
        expected_first_name_level_1_m.as_record_dict(), key=lambda r: r["iteration"]
    )

    for expected, actual in zip(expecteds, actuals):
        assert actual["m_probability"] == pytest.approx(expected["m_probability"])


def test_splink_2_em_no_fix():
    settings_dict = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register_from_csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv"
    )

    linker = Linker(df_sdf, settings_dict)

    # Check lambda history is the same
    expected_prop_history = db_api.register_from_csv(
        "tests/datasets/splink2_proportion_of_matches_history_no_fix.csv"
    )

    training_session = (
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.surname = r.surname", fix_u_probabilities=False
        )
    )
    actual_prop_history = db_api.register(training_session._lambda_history_records)
    actuals = sorted(actual_prop_history.as_record_dict(), key=lambda r: r["iteration"])
    expecteds = sorted(
        expected_prop_history.as_record_dict(), key=lambda r: r["iteration"]
    )

    for expected, actual in zip(expecteds, actuals):
        assert expected["λ"] == pytest.approx(
            actual["probability_two_random_records_match"]
        )

    # Check history of m probabilities is the same for a column
    expected_m_u_history = db_api.register_from_csv(
        "tests/datasets/splink2_m_u_history_no_fix.csv"
    )
    expected_first_name_level_1_m = expected_m_u_history.query_sql(
        """
            SELECT *
            FROM {this}
            WHERE gamma_column_name = 'gamma_first_name'
            AND comparison_vector_value = 1
        """
    )

    actual_m_u_history = db_api.register(
        list(map(lambda r: r.as_dict(), training_session._iteration_history_records))
    )
    actual_first_name_level_1_m = actual_m_u_history.query_sql(
        """
            SELECT *
            FROM {this}
            WHERE comparison_name = 'first_name'
            AND comparison_vector_value = 1
        """
    )

    actuals = sorted(
        actual_first_name_level_1_m.as_record_dict(), key=lambda r: r["iteration"]
    )
    expecteds = sorted(
        expected_first_name_level_1_m.as_record_dict(), key=lambda r: r["iteration"]
    )

    for expected, actual in zip(expecteds, actuals):
        assert actual["m_probability"] == pytest.approx(expected["m_probability"])


def test_lambda():
    # Needs precisely 10 EM iterations
    settings_dict = get_settings_dict()
    settings_dict["max_iterations"] = 10
    settings_dict["em_convergence"] = 1e-10

    bf_for_first_name = 0.9 / 0.1
    glo = bayes_factor_to_prob(prob_to_bayes_factor(0.3) / bf_for_first_name)

    settings_dict["probability_two_random_records_match"] = glo

    db_api = DuckDBAPI()
    df_sdf = db_api.register_from_csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv"
    )

    linker = Linker(df_sdf, settings_dict)

    _ma = linker.inference.predict()
    _training_session = (
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.dob = r.dob", fix_u_probabilities=False
        )
    )
    #########

    bf_for_first_name = (
        linker._settings_obj._get_comparison_by_output_column_name("first_name")
        ._get_comparison_level_by_comparison_vector_value(2)
        ._bayes_factor
    )
    bf_for_surname = (
        linker._settings_obj._get_comparison_by_output_column_name("surname")
        ._get_comparison_level_by_comparison_vector_value(1)
        ._bayes_factor
    )
    glo = bayes_factor_to_prob(
        prob_to_bayes_factor(0.3) / (bf_for_first_name * bf_for_surname)
    )

    for cc in linker._settings_obj.comparisons:
        if cc.output_column_name not in ("first_name", "surname"):
            cl = cc._get_comparison_level_by_comparison_vector_value(1)
            cl.m_probability = 0.9
            cl.u_probability = 0.1
            cl = cc._get_comparison_level_by_comparison_vector_value(0)
            cl.m_probability = 0.1
            cl.u_probability = 0.9

    linker._settings_obj._probability_two_random_records_match = glo

    _training_session = (
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.first_name = r.first_name and l.surname = r.surname",
            fix_u_probabilities=False,
            populate_probability_two_random_records_match_from_trained_values=True,
        )
    )

    # linker._settings_obj.match_weights_chart()

    # from splink.misc import bayes_factor_to_prob, prob_to_bayes_factor

    # The model that blocks on DOB has probability_two_random_records_match of
    # 0.588699831556479

    # The bayes factor for dob is 1.6321361225311535

    # bf = prob_to_bayes_factor(0.588699831556479)
    # bf2 = 1.6321361225311535
    # p = bayes_factor_to_prob(bf/bf2)
    # 0.46722294374907014  (same result from
    # _estimate_global_lambda_from_blocking_specific_lambda in Splink2)

    # The model that blocks on surname and first name has a
    # probability_two_random_records_match of 0.5876227881218818

    # The first name comparison column has bf of 71.435024344641
    # The surname comparison column has bf of 8.378038065716774

    # bf = prob_to_bayes_factor(0.5876227881218818)
    # bf2 = 71.435024344641 * 8.378038065716774
    # p = bayes_factor_to_prob(bf/bf2)
    # p = 0.0023752954691593103
    actual = linker._settings_obj._probability_two_random_records_match
    expected = (1 / 0.46722294374907014 + 1 / 0.0023752954691593103) / 2
    assert actual == pytest.approx(1 / expected)


# The following code generates the comparisons in Splink 2 which are used in these tets

# import pandas as pd
# from utility_functions.demo_utils import get_spark

# spark = get_spark()

# df = spark.read.csv("data/fake_1000.csv")


# case_expr = """
# CASE
# WHEN first_name_l IS NULL OR first_name_r IS NULL THEN -1
# WHEN first_name_l = first_name_r THEN 2
# WHEN levenshtein(first_name_l, first_name_r) <= 2 THEN 1
# ELSE 0 END as gamma_first_name"""

# settings = {
#     "probability_two_random_records_match": 0.3,
#     "link_type": "dedupe_only",
#     "blocking_rules": ["l.surname = r.surname"],
#     "comparisons": [
#         {
#             "col_name": "first_name",
#             "term_frequency_adjustments": True,
#             "fix_u_probabilities": True,
#             "m_probabilities": [0.1, 0.2, 0.7],
#             "u_probabilities": [0.8, 0.1, 0.1],
#             "tf_adjustment_weights": [0, 0, 0.6],
#             "case_expression": case_expr,
#             "num_levels": 3,
#         },
#         {"col_name": "dob", "fix_u_probabilities": True},
#         {"col_name": "city", "fix_u_probabilities": True},
#         {"col_name": "email", "fix_u_probabilities": True},
#     ],
#     "additional_columns_to_retain": ["cluster"],
#     "em_convergence": 0.00001,
#     "max_iterations": 2,
#     "retain_matching_columns": True,
#     "retain_intermediate_calculation_columns": True,
# }

# from splink import Splink

# linker = Splink(settings, df, spark)

# df_e = linker.manually_apply_fellegi_sunter_weights()

# df_e_pd = df_e.filter("unique_id_l = 479").filter("unique_id_r = 481").toPandas()
# df_e_pd.to_csv("splink2_479_vs_481.csv", index=False)

# linker = Splink(settings, df, spark)
# df_e = linker.get_scored_comparisons()
# model = linker.model

# df = pd.DataFrame(model.m_u_history_as_rows())
# df.to_csv("splink2_m_u_history.csv", index=False)

# df2 = pd.DataFrame(model.lambda_history_as_rows())

# f1 = df["gamma_column_name"] == "gamma_first_name"
# f2 = df["comparison_vector_value"] == "1"
