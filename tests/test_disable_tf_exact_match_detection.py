import pandas as pd
import pytest

import splink.internals.comparison_level_library as cll
from splink import DuckDBAPI, Linker, SettingsCreator


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


def test_with_predict_calculation():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    df = df[df["unique_id"].isin([835, 836, 147, 975])]

    def get_settings(disable_tf_exact_match_detection, tf_minimum_u_value=None):
        comparison_level = {
            "sql_condition": 'levenshtein("surname_l", "surname_r") <= 1',
            "label_for_charts": "Levenshtein <= 1",
            "tf_adjustment_column": "surname",
            "u_probability": 0.3,
            "m_probability": 0.9,
        }
        if disable_tf_exact_match_detection:
            comparison_level["disable_tf_exact_match_detection"] = True

        if tf_minimum_u_value is not None:
            comparison_level["tf_minimum_u_value"] = tf_minimum_u_value

        return SettingsCreator(
            link_type="dedupe_only",
            comparisons=[
                {
                    "output_column_name": "surname",
                    "comparison_levels": [
                        cll.NullLevel("surname"),
                        {
                            "sql_condition": '"surname_l" = "surname_r"',
                            "label_for_charts": "Exact match",
                            "tf_adjustment_column": "surname",
                            "u_probability": 0.1,
                            "m_probability": 0.8,
                        },
                        comparison_level,
                        cll.ElseLevel(),
                    ],
                    "comparison_description": "surname description",
                }
            ],
            retain_intermediate_calculation_columns=True,
        )

    settings_normal = get_settings(disable_tf_exact_match_detection=False)

    linker = Linker(df, settings_normal, DuckDBAPI())

    tf_lookup = [
        {"surname": "Taylor", "tf_surname": 0.4},
        {"surname": "Kirk", "tf_surname": 0.2},
    ]
    linker.table_management.register_term_frequency_lookup(tf_lookup, "surname")

    df_predict = linker.inference.predict()

    sql = f"""
    select * from {df_predict.physical_name}
    where unique_id_l = 835
    and unique_id_r = 836
    """
    res = linker.misc.query_sql(sql).to_dict(orient="records")[0]

    # Exact match, normal tf adjustement, Kirk
    assert res["bf_surname"] == pytest.approx(8.0)
    # Overall BF should be m/u = 0.8/0.2 = 4
    assert res["bf_tf_adj_surname"] * res["bf_surname"] == pytest.approx(4.0)

    # Levenshtein match, normal tf adustments
    sql = f"""
    select * from {df_predict.physical_name}
    where unique_id_l = 147
    and unique_id_r = 975
    """
    res = linker.misc.query_sql(sql).to_dict(orient="records")[0]
    # Levenshtein match, normal tf adustments, Taylor
    # Splink makes the tf adjustment based on on the exact match level
    # Lev match level has bf of 0.9/0.3
    assert res["bf_surname"] == pytest.approx(3.0)
    # Overall BR should be based base of 3.0
    # TF adjustmeent is difference between:
    #    u of exact match = 0.1
    #    u of Taylor = 0.4
    assert res["bf_surname"] * res["bf_tf_adj_surname"] == pytest.approx(
        3.0 * (0.1 / 0.4)
    )

    settings_disabled = get_settings(disable_tf_exact_match_detection=True)
    linker = Linker(df, settings_disabled, DuckDBAPI())

    tf_lookup = [
        {"surname": "Taylor", "tf_surname": 0.4},
        {"surname": "Kirk", "tf_surname": 0.2},
    ]
    linker.table_management.register_term_frequency_lookup(tf_lookup, "surname")

    df_predict = linker.inference.predict()

    sql = f"""
    select * from {df_predict.physical_name}
    where unique_id_l = 835
    and unique_id_r = 836
    """
    res = linker.misc.query_sql(sql).to_dict(orient="records")[0]
    # Exact match, normal tf adjustement, Kirk
    assert res["bf_surname"] == pytest.approx(8.0)
    # Overall BF should be m/u = 0.8/0.2 = 4
    assert res["bf_tf_adj_surname"] * res["bf_surname"] == pytest.approx(4.0)

    sql = f"""
    select * from {df_predict.physical_name}
    where unique_id_l = 147
    and unique_id_r = 975
    """
    res = linker.misc.query_sql(sql).to_dict(orient="records")[0]
    # Levenshtein match, tf exact match detection disabled, Taylor
    # Splink makes the tf adjustment based on on the exact match level
    # Lev match level has bf of 0.9/0.3
    assert res["bf_surname"] == pytest.approx(3.0)

    # Overall BR should be based base of 3.0
    # TF adjustmeent is difference between:
    #    u of this level = 0.3
    #    u of Taylor = 0.4
    assert res["bf_surname"] * res["bf_tf_adj_surname"] == pytest.approx(
        3.0 * (0.3 / 0.4)
    )

    settings_disabled_with_min_tf = get_settings(
        disable_tf_exact_match_detection=True, tf_minimum_u_value=0.1
    )

    linker_base = Linker(df, settings_disabled_with_min_tf, DuckDBAPI())
    linkers = [
        linker_base,
        Linker(df, linker_base.misc.save_model_to_json(), DuckDBAPI()),
    ]

    # This ensures we're checking that serialisation and deserialisation
    # works on the disable_tf_exact_match_detection and tf_minimum_u_value settings
    for linker in linkers:
        tf_lookup = [
            {"surname": "Taylor", "tf_surname": 0.001},
            {"surname": "Kirk", "tf_surname": 0.2},
        ]
        linker.table_management.register_term_frequency_lookup(tf_lookup, "surname")

        df_predict = linker.inference.predict()

        sql = f"""
        select * from {df_predict.physical_name}
        where unique_id_l = 835
        and unique_id_r = 836
        """
        res = linker.misc.query_sql(sql).to_dict(orient="records")[0]
        # Exact match, normal tf adjustement, Kirk
        assert res["bf_surname"] == pytest.approx(8.0)
        # Overall BF should be m/u = 0.8/0.2 = 4
        assert res["bf_tf_adj_surname"] * res["bf_surname"] == pytest.approx(4.0)

        sql = f"""
        select * from {df_predict.physical_name}
        where unique_id_l = 147
        and unique_id_r = 975
        """
        res = linker.misc.query_sql(sql).to_dict(orient="records")[0]
        # Levenshtein match, tf exact match detection disabled, Taylor
        # Splink makes the tf adjustment based on on the exact match level
        # Lev match level has bf of 0.9/0.3
        assert res["bf_surname"] == pytest.approx(3.0)

        # Overall BR should be based base of 3.0
        # TF adjustmeent is difference between:
        #    u of this level = 0.3
        #    u of Taylor with min tf applied = 0.1
        assert res["bf_surname"] * res["bf_tf_adj_surname"] == pytest.approx(
            3.0 * (0.3 / 0.1)
        )
