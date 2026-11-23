import pytest

import splink.comparison_library as cl
from splink import SettingsCreator, block_on
from tests.decorator import mark_with_dialects_including

data = [
    {
        "unique_id": 1,
        "first_name": "Alice",
        "surname": "Smith",
        "dob": "1990",
        "city": "London",
    },
    {
        "unique_id": 2,
        "first_name": "Alice",
        "surname": "Jones",
        "dob": "1990",
        "city": "Leeds",
    },
    {
        "unique_id": 3,
        "first_name": "Bob",
        "surname": "Smith",
        "dob": "1992",
        "city": "Bristol",
    },
    {
        "unique_id": 4,
        "first_name": "Carol",
        "surname": "Green",
        "dob": "1993",
        "city": "Cardiff",
    },
]


def _linker_and_predictions(test_helpers):
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("surname"),
            block_on("dob"),
            block_on("city"),
        ],
    )
    linker = test_helpers["duckdb"].linker_with_registration([data], settings)
    df_predict = linker._db_api.register(
        [
            {
                "unique_id_l": 1,
                "unique_id_r": 2,
                "match_probability": 0.25,
            },
            {
                "unique_id_l": 1,
                "unique_id_r": 3,
                "match_probability": 0.75,
            },
        ]
    )
    return linker, df_predict


@mark_with_dialects_including("duckdb")
def test_blocking_rule_importance_chart_data(test_helpers):
    linker, df_predict = _linker_and_predictions(test_helpers)
    created_tables_before = set(linker._db_api._created_tables)

    chart = linker.blocking_analysis.chart_blocking_rule_importance(df_predict)

    assert chart.raw_records == [
        {
            "blocking_rule_index": 0,
            "blocking_rule": 'l."first_name" = r."first_name"',
            "comparison_count": 1,
            "overlapping_comparison_count": 1,
            "marginal_comparison_count": 0,
            "estimated_marginal_match_count": 0.0,
            "is_redundant": True,
        },
        {
            "blocking_rule_index": 1,
            "blocking_rule": 'l."surname" = r."surname"',
            "comparison_count": 1,
            "overlapping_comparison_count": 0,
            "marginal_comparison_count": 1,
            "estimated_marginal_match_count": 0.75,
            "is_redundant": False,
        },
        {
            "blocking_rule_index": 2,
            "blocking_rule": 'l."dob" = r."dob"',
            "comparison_count": 1,
            "overlapping_comparison_count": 1,
            "marginal_comparison_count": 0,
            "estimated_marginal_match_count": 0.0,
            "is_redundant": True,
        },
        {
            "blocking_rule_index": 3,
            "blocking_rule": 'l."city" = r."city"',
            "comparison_count": 0,
            "overlapping_comparison_count": 0,
            "marginal_comparison_count": 0,
            "estimated_marginal_match_count": 0.0,
            "is_redundant": True,
        },
    ]
    assert set(linker._db_api._created_tables) == created_tables_before


@mark_with_dialects_including("duckdb")
def test_blocking_rule_importance_validates_predictions(test_helpers):
    linker, df_predict = _linker_and_predictions(test_helpers)

    filtered_predictions = df_predict.query_sql(
        "SELECT * FROM {this} WHERE unique_id_l = 1 AND unique_id_r = 2"
    )
    try:
        with pytest.raises(ValueError, match="does not contain all pairs.*Missing 1"):
            linker.blocking_analysis.blocking_rule_importance(filtered_predictions)
    finally:
        filtered_predictions.drop_table_from_database_and_remove_from_cache()

    duplicate_predictions = df_predict.query_sql(
        "SELECT * FROM {this} UNION ALL SELECT * FROM {this}"
    )
    try:
        with pytest.raises(ValueError, match="duplicate rows for 2 blocked pairs"):
            linker.blocking_analysis.blocking_rule_importance(duplicate_predictions)
    finally:
        duplicate_predictions.drop_table_from_database_and_remove_from_cache()

    invalid_predictions = df_predict.query_sql(
        "SELECT * REPLACE (NULL AS match_probability) FROM {this}"
    )
    try:
        with pytest.raises(ValueError, match="2 invalid match_probability values"):
            linker.blocking_analysis.blocking_rule_importance(invalid_predictions)
    finally:
        invalid_predictions.drop_table_from_database_and_remove_from_cache()
