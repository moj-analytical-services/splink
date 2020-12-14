import pytest

from pyspark.sql import Row

from splink import Splink


def test_fix_u(spark, link_dedupe_data):
    settings = {
        "link_type": "link_only",
        "comparison_columns": [{"col_name": "first_name"}, {"col_name": "surname"}],
        "blocking_rules": [],
    }

    # We expect u on the cartesian product of MoB to be around
    df = [
        {"unique_id": 1, "mob": "1", "first_name": "a", "surname": "a"},
        {"unique_id": 2, "mob": "2", "first_name": "b", "surname": "b"},
        {"unique_id": 3, "mob": "3", "first_name": "c", "surname": "c"},
        {"unique_id": 4, "mob": "4", "first_name": "d", "surname": "d"},
        {"unique_id": 5, "mob": "5", "first_name": "e", "surname": "e"},
        {"unique_id": 6, "mob": "6", "first_name": "f", "surname": "f"},
        {"unique_id": 7, "mob": "7", "first_name": "g", "surname": "g"},
        {"unique_id": 9, "mob": "9", "first_name": "h", "surname": "h"},
        {"unique_id": 10, "mob": "10", "first_name": "i", "surname": "i"},
        {"unique_id": 10, "mob": "10", "first_name": "i", "surname": "i"},
    ]

    df = spark.createDataFrame(Row(**x) for x in df)

    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.1,
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "u_probabilities": [0.8, 0.2],
                "fix_u_probabilities": True,
            },
            {
                "col_name": "first_name",
                "u_probabilities": [0.8, 0.2],
            },
            {"col_name": "surname"},
        ],
        "blocking_rules": [],
        "max_iterations": 3,
    }

    linker = Splink(settings, spark, df=df)

    df_e = linker.get_scored_comparisons()

    # Want to check that the "u_probabilities" in the latest parameters are still 0.8, 0.2
    mob = linker.params.params["π"]["gamma_mob"]["prob_dist_non_match"]
    assert mob["level_0"]["probability"] == pytest.approx(0.8)
    assert mob["level_1"]["probability"] == pytest.approx(0.2)

    first_name = linker.params.params["π"]["gamma_first_name"]["prob_dist_non_match"]
    assert first_name["level_0"]["probability"] != pytest.approx(0.8)
    assert first_name["level_1"]["probability"] != pytest.approx(0.2)

    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.1,
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "u_probabilities": [0.8, 0.2],
                "fix_u_probabilities": False,
            },
            {"col_name": "first_name"},
            {"col_name": "surname"},
        ],
        "blocking_rules": [],
        "max_iterations": 3,
    }

    linker = Splink(settings, spark, df=df)

    df_e = linker.get_scored_comparisons()

    # Want to check that the "u_probabilities" in the latest parameters are no longer 0.8, 0.2
    mob = linker.params.params["π"]["gamma_mob"]["prob_dist_non_match"]
    assert mob["level_0"]["probability"] != pytest.approx(0.8)
    assert mob["level_1"]["probability"] != pytest.approx(0.2)

    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.1,
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "m_probabilities": [0.04, 0.96],
                "fix_m_probabilities": True,
                "u_probabilities": [0.75, 0.25],
                "fix_u_probabilities": False,
            },
            {"col_name": "first_name"},
            {"col_name": "surname"},
        ],
        "blocking_rules": [],
        "max_iterations": 3,
    }

    linker = Splink(settings, spark, df=df)

    df_e = linker.get_scored_comparisons()

    mob = linker.params.params["π"]["gamma_mob"]["prob_dist_non_match"]
    assert mob["level_0"]["probability"] != pytest.approx(0.75)
    assert mob["level_1"]["probability"] != pytest.approx(0.25)

    mob = linker.params.params["π"]["gamma_mob"]["prob_dist_match"]
    assert mob["level_0"]["probability"] == pytest.approx(0.04)
    assert mob["level_1"]["probability"] == pytest.approx(0.96)