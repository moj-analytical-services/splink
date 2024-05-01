import splink.duckdb.comparison_level_library as cll
from splink.comparison import Comparison
from splink.settings import Settings


def test_disable_tf_exact_match_detection():
    settings = Settings({"link_type": "dedupe_only"})

    comparison_normal_dict = {
        "output_column_name": "my_col",
        "comparison_levels": [
            cll.null_level("my_col"),
            {
                "sql_condition": '"my_col_l" = "my_col_r"',
                "label_for_charts": "Exact match",
                "tf_adjustment_column": "my_col",
                "u_probability": 0.123,
            },
            {
                "sql_condition": 'levenshtein("my_col_l", "my_col_r") <= 1',
                "label_for_charts": "Levenshtein <= 1",
                "tf_adjustment_column": "my_col",
                "u_probability": 0.234,
            },
            cll.else_level(),
        ],
        "comparison_description": "my_col",
    }
    comparison_normal = Comparison(comparison_normal_dict)
    comparison_normal._settings_obj = settings
    comparison_level_exact_match = comparison_normal.comparison_levels[1]
    assert (
        comparison_level_exact_match._u_probability_corresponding_to_exact_match
        == 0.123
    )
    assert "0.123" in comparison_level_exact_match._tf_adjustment_sql
    assert "0.234" not in comparison_level_exact_match._tf_adjustment_sql

    comparison_level_levenshtein = comparison_normal.comparison_levels[2]
    assert (
        comparison_level_levenshtein._u_probability_corresponding_to_exact_match
        == 0.123
    )
    assert "0.123" in comparison_level_levenshtein._tf_adjustment_sql
    assert "0.234" not in comparison_level_levenshtein._tf_adjustment_sql

    arr_sql = 'array_length(list_intersect("test_array_l", "test_array_r"))>= 1'
    comparison_disabled_dict = {
        "output_column_name": "my_col",
        "comparison_levels": [
            cll.null_level("my_col"),
            {
                "sql_condition": arr_sql,
                "label_for_charts": "Exact match",
                "tf_adjustment_column": "my_col",
                "u_probability": 0.456,
                "disable_tf_exact_match_detection": True,
            },
            cll.else_level(),
        ],
        "comparison_description": "arr",
    }
    comparison_disabled = Comparison(comparison_disabled_dict)
    comparison_disabled._settings_obj = settings

    exact_match = comparison_disabled.comparison_levels[1]

    assert exact_match._u_probability_corresponding_to_exact_match == 0.456
    assert "0.456" in exact_match._tf_adjustment_sql
