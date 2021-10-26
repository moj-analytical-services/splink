from splink import Splink, load_from_json
from pyspark.sql import Row
from splink.intuition import intuition_report, bayes_factor_chart


from splink_data_generation.generate_data_exact import generate_df_gammas_exact
from splink_data_generation.match_prob import add_match_prob

from splink_data_generation.estimate_splink import estimate

import pytest
from copy import deepcopy


@pytest.mark.skip(reason="This is a very long running test of iteration")
def test_splink_converges_to_known_params(spark):
    settings_for_data_generation = {
        "link_type": "link_and_dedupe",
        "comparison_columns": [
            {
                "col_name": "col_1",
                "m_probabilities": [0.3, 0.7],  # Probability of typo
                "u_probabilities": [0.9, 0.1],  # Probability of collision
            },
            {
                "col_name": "col_2",
                "m_probabilities": [0.1, 0.9],  # Probability of typo
                "u_probabilities": [0.975, 0.025],  # Probability of collision
            },
            {
                "col_name": "col_3",
                "m_probabilities": [0.05, 0.95],  # Probability of typo
                "u_probabilities": [0.8, 0.2],  # Probability of collision
            },
        ],
    }

    df = generate_df_gammas_exact(settings_for_data_generation)
    df = add_match_prob(df, settings_for_data_generation)

    # Now use Splink to estimate the params from the data
    settings = {
        "link_type": "link_and_dedupe",
        "comparison_columns": [
            {
                "col_name": "col_1",
            },
            {
                "col_name": "col_2",
            },
            {
                "col_name": "col_3",
            },
        ],
        "max_iterations": 200,
        "em_convergence": 0.00001,
        "additional_columns_to_retain": ["true_match", "true_match_probability"],
        "retain_intermediate_calculation_columns": True,
    }

    df_e, linker = estimate(df, settings, spark)

    estimated_settings = linker.model.current_settings_obj.settings_dict

    cc_actual = settings_for_data_generation["comparison_columns"]
    cc_estimated = estimated_settings["comparison_columns"]
    ccs = zip(cc_actual, cc_estimated)

    for (col_actual, col_estimated) in ccs:
        for p in ["m_probabilities", "u_probabilities"]:
            assert col_actual[p] == pytest.approx(col_estimated[p], abs=0.001)


def test_splink_does_not_converge_away_from_correct_params(spark):
    settings = {
        "proportion_of_matches": 0.5,
        "link_type": "dedupe_only",
        "comparison_columns": [
            {
                "col_name": "col_1",
                "m_probabilities": [0.3, 0.7],  # Probability of typo
                "u_probabilities": [0.9, 0.1],  # Probability of collision
            },
            {
                "col_name": "col_2",
                "m_probabilities": [0.1, 0.9],  # Probability of typo
                "u_probabilities": [0.975, 0.025],  # Probability of collision
            },
            {
                "col_name": "col_3",
                "m_probabilities": [0.05, 0.95],  # Probability of typo
                "u_probabilities": [0.8, 0.2],  # Probability of collision
            },
        ],
        "max_iterations": 10,
        "em_convergence": 0.00001,
        "additional_columns_to_retain": ["true_match", "true_match_probability"],
    }
    actual_settings = deepcopy(settings)

    df = generate_df_gammas_exact(settings)
    df = add_match_prob(df, settings)

    df_e, model = estimate(df, actual_settings, spark)

    estimated_settings = model.current_settings_obj.settings_dict

    cc_actual = actual_settings["comparison_columns"]
    cc_estimated = estimated_settings["comparison_columns"]
    ccs = zip(cc_actual, cc_estimated)

    for (col_actual, col_estimated) in ccs:
        for p in ["m_probabilities", "u_probabilities"]:
            assert col_actual[p] == pytest.approx(col_estimated[p], abs=0.001)


def test_main_api(spark):

    rows = [
        {"unique_id": 1, "mob": 10, "surname": "Linacre"},
        {"unique_id": 2, "mob": 10, "surname": "Linacre"},
        {"unique_id": 3, "mob": 10, "surname": "Linacer"},
        {"unique_id": 4, "mob": 7, "surname": "Smith"},
        {"unique_id": 5, "mob": 8, "surname": "Smith"},
        {"unique_id": 6, "mob": 8, "surname": "Smith"},
        {"unique_id": 7, "mob": 8, "surname": "Jones"},
    ]

    df = spark.createDataFrame(Row(**x) for x in rows)

    settings = {
        "link_type": "dedupe_only",
        "comparison_columns": [{"col_name": "surname"}, {"col_name": "mob"}],
        "blocking_rules": ["l.mob = r.mob", "l.surname = r.surname"],
        "max_iterations": 1,
    }

    linker = Splink(settings, df, spark)
    df_e = linker.get_scored_comparisons()
    linker.save_model_as_json("saved_model.json", overwrite=True)
    linker_2 = load_from_json("saved_model.json", df, spark=spark)
    df_e = linker_2.get_scored_comparisons()

    model = linker.model
    row_dict = df_e.toPandas().sample(1).to_dict(orient="records")[0]
    intuition_report(row_dict, model)
    bayes_factor_chart(row_dict, model)
