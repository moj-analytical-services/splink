import splink.comparison_level_library as cll
from splink.settings_creator import SettingsCreator


def test_disable_tf_exact_match_detection():

    comparison_normal_dict = {
        "output_column_name": "my_col",
        "comparison_levels": [
            cll.NullLevel("my_col"),
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
            cll.ElseLevel(),
        ],
        "comparison_description": "my_col",
    }
    # ci = ColumnInfoSettings("bf", "tf", "gamma_", "unique_id", None, False, "duckdb")

    settings = SettingsCreator(
        link_type="dedupe_only", comparisons=[comparison_normal_dict]
    ).get_settings("duckdb")
    comparison_normal = settings.comparisons[0]
    comparison_levels_normal = comparison_normal.comparison_levels
    comparison_level_exact_match = comparison_levels_normal[1]

    assert (
        comparison_level_exact_match._u_probability_corresponding_to_exact_match(
            comparison_levels_normal
        )
        == 0.123
    )
    assert "0.123" in comparison_level_exact_match._tf_adjustment_sql(
        "gamma_", comparison_levels_normal
    )
    assert "0.234" not in comparison_level_exact_match._tf_adjustment_sql(
        "gamma_", comparison_levels_normal
    )

    comparison_level_levenshtein = comparison_levels_normal[2]
    assert (
        comparison_level_levenshtein._u_probability_corresponding_to_exact_match(
            comparison_levels_normal
        )
        == 0.123
    )
    assert "0.123" in comparison_level_levenshtein._tf_adjustment_sql(
        "gamma_", comparison_levels_normal
    )
    assert "0.234" not in comparison_level_levenshtein._tf_adjustment_sql(
        "gamma_", comparison_levels_normal
    )

    arr_sql = 'array_length(list_intersect("test_array_l", "test_array_r"))>= 1'
    comparison_disabled_dict = {
        "output_column_name": "my_col",
        "comparison_levels": [
            cll.NullLevel("my_col"),
            {
                "sql_condition": arr_sql,
                "label_for_charts": "Exact match",
                "tf_adjustment_column": "my_col",
                "u_probability": 0.456,
                "disable_tf_exact_match_detection": True,
            },
            cll.ElseLevel(),
        ],
        "comparison_description": "arr",
    }

    settings = SettingsCreator(
        link_type="dedupe_only", comparisons=[comparison_disabled_dict]
    ).get_settings("duckdb")
    comparison_disabled = settings.comparisons[0]
    comparison_levels_disabled = comparison_disabled.comparison_levels
    comparison_level_exact_match = comparison_levels_disabled[1]

    exact_match = comparison_disabled.comparison_levels[1]

    assert (
        exact_match._u_probability_corresponding_to_exact_match(
            comparison_levels_disabled
        )
        == 0.456
    )
    assert "0.456" in exact_match._tf_adjustment_sql(
        "gamma_", comparison_levels_disabled
    )
