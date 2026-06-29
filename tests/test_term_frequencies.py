import pytest

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
from splink.internals.misc import match_weight_to_bayes_factor


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

    return data


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
    return {
        "London": df_predict.query_sql(
            "SELECT * FROM {this} WHERE city_l = 'London'"
        ).as_record_list()[0],
        "Birmingham": df_predict.query_sql(
            "SELECT * FROM {this} WHERE city_l = 'Birmingham'"
        ).as_record_list()[0],
        "Truro": df_predict.query_sql(
            "SELECT * FROM {this} WHERE city_l = 'Truro'"
        ).as_record_list()[0],
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

    db_api = DuckDBAPI(connection=":memory:")
    data_sdf = db_api.register(data)
    linker = Linker(data_sdf, settings)
    df_predict = linker.inference.predict()
    results = filter_results(df_predict)

    bf_no_adj = match_weight_to_bayes_factor(results["London"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["London"]["mw_tf_adj_city"])
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf_no_adj) == 5.0
    assert pytest.approx(bf) == 50 / 40  # 40/50 or 80% of values are london

    bf_no_adj = match_weight_to_bayes_factor(results["Birmingham"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["Birmingham"]["mw_tf_adj_city"])
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf) == 50 / 8

    bf_no_adj = match_weight_to_bayes_factor(results["Truro"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["Truro"]["mw_tf_adj_city"])
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

    db_api = DuckDBAPI(connection=":memory:")
    data_sdf = db_api.register(data)
    linker = Linker(data_sdf, settings)
    df_predict = linker.inference.predict()
    results = filter_results(df_predict)

    bf_no_adj = match_weight_to_bayes_factor(results["London"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["London"]["mw_tf_adj_city"])
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf_no_adj) == 5.0
    assert pytest.approx(bf) == 50 / 40  # 40/50 or 80% of values are london

    bf_no_adj = match_weight_to_bayes_factor(results["Birmingham"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["Birmingham"]["mw_tf_adj_city"])
    bf = bf_no_adj * bf_adj
    assert pytest.approx(bf) == 50 / 8

    bf_no_adj = match_weight_to_bayes_factor(results["Truro"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["Truro"]["mw_tf_adj_city"])
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

    db_api = DuckDBAPI(connection=":memory:")

    data_sdf = db_api.register(data)
    linker = Linker(data_sdf, settings)
    df_predict = linker.inference.predict()
    results = filter_results(df_predict)

    bf_no_adj = match_weight_to_bayes_factor(results["London"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["London"]["mw_tf_adj_city"])
    bf = bf_no_adj * bf_adj

    # Expected value is 5.0 for no adjust
    # With no weighting, target value is 1.25
    # Adjustment would be 5.0/1.25 = 0.25 if no weighting was applied

    assert pytest.approx(bf) == bf_no_adj * 0.25**0.5

    bf_no_adj = match_weight_to_bayes_factor(results["Birmingham"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["Birmingham"]["mw_tf_adj_city"])
    bf = bf_no_adj * bf_adj

    # With no weighting, target value is 6.25
    # Adjustment would be 6.25/5.0 = 1.25 if no weighting was applied

    assert pytest.approx(bf) == bf_no_adj * 1.25**0.5

    bf_no_adj = match_weight_to_bayes_factor(results["Truro"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["Truro"]["mw_tf_adj_city"])
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

    db_api = DuckDBAPI(connection=":memory:")

    data_sdf = db_api.register(data)
    linker = Linker(data_sdf, settings)
    df_predict = linker.inference.predict()
    results = filter_results(df_predict)

    bf_no_adj = match_weight_to_bayes_factor(results["London"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["London"]["mw_tf_adj_city"])
    bf = bf_no_adj * bf_adj

    bf_no_adj = match_weight_to_bayes_factor(results["Truro"]["mw_city"])
    bf_adj = match_weight_to_bayes_factor(results["Truro"]["mw_tf_adj_city"])
    bf = bf_no_adj * bf_adj

    # With no weighting, target value is 4.0
    # Adjustment would be 10/5.0 = 2 if no weighting was applied

    assert pytest.approx(bf) == bf_no_adj * 2**0.5


def test_tf_missing_values_in_lookup():
    """Test that missing TF values in lookup table don't cause errors
    and fall back to no adjustment (mw_tf_adj = 0.0)"""

    # Create data with cities where Paris won't be in the TF table
    data = [
        {"unique_id": 1, "city": "London"},
        {"unique_id": 2, "city": "London"},
        {"unique_id": 3, "city": "Paris"},
        {"unique_id": 4, "city": "Paris"},
    ]

    city_comparison = get_city_comparison()

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [city_comparison],
        "blocking_rules_to_generate_predictions": ["l.city = r.city"],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    db_api = DuckDBAPI(connection=":memory:")
    data_sdf = db_api.register(data)
    linker = Linker(data_sdf, settings)

    # Register only London in the TF table - Paris is intentionally missing
    # u_base = 0.2, tf_london = 0.1 (half u_base), so adj mw = log2(0.2/0.1) = 1.0
    tf_data = [
        {
            "city": "London",
            "tf_city": 0.1,  # Half the u_base value of 0.2
        },
    ]

    tf_data_sdf = db_api.register(tf_data)
    linker.table_management.register_term_frequency_lookup(tf_data_sdf, "city")

    df_predict = linker.inference.predict()

    # Get results for each city
    london_result = df_predict.query_sql(
        "SELECT * FROM {this} WHERE city_l = 'London' AND city_r = 'London'"
    ).as_record_list()[0]
    paris_result = df_predict.query_sql(
        "SELECT * FROM {this} WHERE city_l = 'Paris' AND city_r = 'Paris'"
    ).as_record_list()[0]

    # London should have TF adjustment of 1.0 (log2(0.2/0.1) = log2(2) = 1.0)
    assert pytest.approx(london_result["mw_tf_adj_city"]) == 1.0

    # Paris should have NO TF adjustment (mw_tf_adj = 0.0)
    assert pytest.approx(paris_result["mw_tf_adj_city"]) == 0.0
