import pandas as pd
import pytest

from splink.duckdb.linker import DuckDBLinker


def get_data():
    city_counts = {
        "London": 40,
        "Birmingham": 8,
        "Truro": 2,
    }

    data = []
    counter = 0
    for city, count in city_counts.items():
        for _ in range(count):
            data.append({"unique_id": counter, "city": city})
            counter += 1

    df = pd.DataFrame(data)
    return df


def get_city_comparison():
    return {
        "comparison_levels": [
            {
                "sql_condition": "city_l IS NULL OR city_r IS NULL",
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": "city_l = city_r",
                "label_for_charts": "Exact match",
                "tf_adjustment_column": "city",
                "m_probability": 1.0,
                "u_probability": 0.2,
            },
            {
                "sql_condition": "ELSE",
                "label_for_charts": "All other comparisons",
                "u_probability": 0.8,
                "m_probability": 0.01,
            },
        ]
    }


def filter_results(df_predict):
    df_e_pd = df_predict.as_pandas_dataframe()

    f_london = df_e_pd["city_l"] == "London"
    df_london = df_e_pd[f_london].head(1)

    f_birmingham = df_e_pd["city_l"] == "Birmingham"
    df_birmingham = df_e_pd[f_birmingham].head(1)

    f_truro = df_e_pd["city_l"] == "Truro"
    df_truro = df_e_pd[f_truro].head(1)

    return {
        "London": df_london.to_dict(orient="records")[0],
        "Birmingham": df_birmingham.to_dict(orient="records")[0],
        "Truro": df_truro.to_dict(orient="records")[0],
    }


def test_tf_basic():
    data = get_data()

    city_comparison = get_city_comparison()

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [city_comparison],
        "blocking_rules_to_generate_predictions": ["l.city = r.city"],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    linker = DuckDBLinker(
        data, settings, connection=":memory:", validate_settings=False
    )
    df_predict = linker.predict()
    results = filter_results(df_predict)

    bf_no_adj = results["London"]["bf_city"]
    bf_adj = results["London"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf_no_adj) == 5.0
    assert pytest.approx(bf) == 50 / 40  # 40/50 or 80% of values are london

    bf_no_adj = results["Birmingham"]["bf_city"]
    bf_adj = results["Birmingham"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf) == 50 / 8

    bf_no_adj = results["Truro"]["bf_city"]
    bf_adj = results["Truro"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf) == 50 / 2


def test_tf_clamp():
    data = get_data()

    city_comparison = get_city_comparison()

    city_comparison["comparison_levels"][1]["tf_minimum_u_value"] = 0.1

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [city_comparison],
        "blocking_rules_to_generate_predictions": ["l.city = r.city"],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    linker = DuckDBLinker(
        data, settings, connection=":memory:", validate_settings=False
    )
    df_predict = linker.predict()
    results = filter_results(df_predict)

    bf_no_adj = results["London"]["bf_city"]
    bf_adj = results["London"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf_no_adj) == 5.0
    assert pytest.approx(bf) == 50 / 40  # 40/50 or 80% of values are london

    bf_no_adj = results["Birmingham"]["bf_city"]
    bf_adj = results["Birmingham"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf) == 50 / 8

    bf_no_adj = results["Truro"]["bf_city"]
    bf_adj = results["Truro"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf) == 10


def test_weight():
    data = get_data()

    city_comparison = get_city_comparison()

    city_comparison["comparison_levels"][1]["tf_adjustment_weight"] = 0.5

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [city_comparison],
        "blocking_rules_to_generate_predictions": ["l.city = r.city"],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    linker = DuckDBLinker(
        data, settings, connection=":memory:", validate_settings=False
    )
    df_predict = linker.predict()
    results = filter_results(df_predict)

    bf_no_adj = results["London"]["bf_city"]
    bf_adj = results["London"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj

    # Expected value is 5.0 for no adjust
    # With no weighting, target value is 1.25
    # Adjustment would be 5.0/1.25 = 0.25 if no weighting was applied

    assert pytest.approx(bf) == bf_no_adj * 0.25**0.5

    bf_no_adj = results["Birmingham"]["bf_city"]
    bf_adj = results["Birmingham"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj

    # With no weighting, target value is 6.25
    # Adjustment would be 6.25/5.0 = 1.25 if no weighting was applied

    assert pytest.approx(bf) == bf_no_adj * 1.25**0.5

    bf_no_adj = results["Truro"]["bf_city"]
    bf_adj = results["Truro"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj

    # With no weighting, target value is 4.0
    # Adjustment would be 25/5.0 = 5 if no weighting was applied

    assert pytest.approx(bf) == bf_no_adj * 5**0.5


def test_weightand_clamp():
    data = get_data()

    city_comparison = get_city_comparison()

    city_comparison["comparison_levels"][1]["tf_adjustment_weight"] = 0.5
    city_comparison["comparison_levels"][1]["tf_minimum_u_value"] = 0.1

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [city_comparison],
        "blocking_rules_to_generate_predictions": ["l.city = r.city"],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    linker = DuckDBLinker(
        data, settings, connection=":memory:", validate_settings=False
    )
    df_predict = linker.predict()
    results = filter_results(df_predict)

    bf_no_adj = results["London"]["bf_city"]
    bf_adj = results["London"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj

    bf_no_adj = results["Truro"]["bf_city"]
    bf_adj = results["Truro"]["bf_tf_adj_city"]
    bf = bf_no_adj * bf_adj

    # With no weighting, target value is 4.0
    # Adjustment would be 10/5.0 = 2 if no weighting was applied

    assert pytest.approx(bf) == bf_no_adj * 2**0.5
