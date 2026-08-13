import pyarrow as pa
import pytest

import splink.comparison_library as cl
from splink import SettingsCreator, block_on
from splink.internals.blocking_rule_importance import _blocking_rule_hits_data
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
    df_predict = linker.inference.predict(warning_mode="never")
    return linker, df_predict


@mark_with_dialects_including("duckdb")
def test_blocking_rule_hits_are_order_independent(test_helpers):
    linker, _ = _linker_and_predictions(test_helpers)
    blocking_rules = linker._settings_obj._blocking_rules_to_generate_predictions

    hits = _blocking_rule_hits_data(linker, blocking_rules)
    try:
        records = hits.as_record_list()
    finally:
        hits.drop_table_from_database_and_remove_from_cache()

    assert len(records) == 2
    records_by_pair = {
        (int(record["join_key_l"]), int(record["join_key_r"])): record
        for record in records
    }
    assert records_by_pair[(1, 2)] == {
        "join_key_l": 1,
        "join_key_r": 2,
        "br_hit_0": 1,
        "br_hit_1": 0,
        "br_hit_2": 1,
        "br_hit_3": 0,
        "number_of_rules_hit": 2,
    }
    assert records_by_pair[(1, 3)] == {
        "join_key_l": 1,
        "join_key_r": 3,
        "br_hit_0": 0,
        "br_hit_1": 1,
        "br_hit_2": 0,
        "br_hit_3": 0,
        "number_of_rules_hit": 1,
    }

    reversed_hits = _blocking_rule_hits_data(
        linker,
        list(reversed(blocking_rules)),
    )
    try:
        reversed_records = reversed_hits.as_record_list()
    finally:
        reversed_hits.drop_table_from_database_and_remove_from_cache()

    reversed_records_by_pair = {
        (int(record["join_key_l"]), int(record["join_key_r"])): record
        for record in reversed_records
    }
    assert set(reversed_records_by_pair) == set(records_by_pair)
    for pair, record in records_by_pair.items():
        reversed_record = reversed_records_by_pair[pair]
        assert reversed_record["number_of_rules_hit"] == record[
            "number_of_rules_hit"
        ]
        for rule_index in range(len(blocking_rules)):
            reversed_rule_index = len(blocking_rules) - rule_index - 1
            assert reversed_record[f"br_hit_{reversed_rule_index}"] == record[
                f"br_hit_{rule_index}"
            ]


@mark_with_dialects_including("duckdb")
def test_blocking_rule_importance_summary(test_helpers):
    linker, df_predict = _linker_and_predictions(test_helpers)
    created_tables_before = set(linker._db_api._created_tables)
    preceding_rules_before = [
        list(rule.preceding_rules)
        for rule in linker._settings_obj._blocking_rules_to_generate_predictions
    ]

    importance = linker.blocking_analysis.blocking_rule_importance(df_predict)
    by_rule_index = {
        record["blocking_rule_index"]: record for record in importance
    }

    assert len(importance) == 4
    assert [
        (
            record["comparison_count"],
            record["overlapping_comparison_count"],
            record["marginal_comparison_count"],
            record["is_redundant"],
        )
        for record in importance
    ] == [
        (1, 1, 0, True),
        (1, 0, 1, False),
        (1, 1, 0, True),
        (0, 0, 0, True),
    ]

    marginal_prediction = next(
        record
        for record in df_predict.as_record_list()
        if {int(record["unique_id_l"]), int(record["unique_id_r"])} == {1, 3}
    )
    assert by_rule_index[1]["estimated_marginal_match_count"] == pytest.approx(
        marginal_prediction["match_probability"]
    )
    assert by_rule_index[0]["estimated_marginal_match_count"] == 0.0
    assert by_rule_index[2]["estimated_marginal_match_count"] == 0.0
    assert by_rule_index[3]["estimated_marginal_match_count"] == 0.0
    assert set(linker._db_api._created_tables) == created_tables_before
    assert [
        rule.preceding_rules
        for rule in linker._settings_obj._blocking_rules_to_generate_predictions
    ] == preceding_rules_before


@mark_with_dialects_including("duckdb")
def test_blocking_rule_importance_requires_complete_unique_predictions(test_helpers):
    linker, df_predict = _linker_and_predictions(test_helpers)

    filtered_predictions = df_predict.query_sql(
        "SELECT * FROM {this} WHERE unique_id_l = 1 AND unique_id_r = 2"
    )
    try:
        created_tables_before = set(linker._db_api._created_tables)
        with pytest.raises(ValueError, match="does not contain all pairs.*Missing 1"):
            linker.blocking_analysis.blocking_rule_importance(filtered_predictions)
        assert set(linker._db_api._created_tables) == created_tables_before
    finally:
        filtered_predictions.drop_table_from_database_and_remove_from_cache()

    duplicate_predictions = df_predict.query_sql(
        "SELECT * FROM {this} UNION ALL SELECT * FROM {this}"
    )
    try:
        created_tables_before = set(linker._db_api._created_tables)
        with pytest.raises(ValueError, match="duplicate rows for 2 blocked pairs"):
            linker.blocking_analysis.blocking_rule_importance(duplicate_predictions)
        assert set(linker._db_api._created_tables) == created_tables_before
    finally:
        duplicate_predictions.drop_table_from_database_and_remove_from_cache()

    invalid_predictions = df_predict.query_sql(
        "SELECT * REPLACE (NULL AS match_probability) FROM {this}"
    )
    try:
        created_tables_before = set(linker._db_api._created_tables)
        with pytest.raises(ValueError, match="2 invalid match_probability values"):
            linker.blocking_analysis.blocking_rule_importance(invalid_predictions)
        assert set(linker._db_api._created_tables) == created_tables_before
    finally:
        invalid_predictions.drop_table_from_database_and_remove_from_cache()


@mark_with_dialects_including("duckdb")
def test_blocking_rule_importance_supports_exploding_rules_and_link_only(
    test_helpers,
):
    data_l = pa.Table.from_pylist(
        [
            {"unique_id": 1, "gender": "m", "postcode": ["2612", "2000"]},
            {"unique_id": 2, "gender": "m", "postcode": ["2612", "2617"]},
            {"unique_id": 3, "gender": "f", "postcode": ["2617"]},
        ]
    )
    data_r = pa.Table.from_pylist(
        [
            {"unique_id": 4, "gender": "m", "postcode": ["2617", "2600"]},
            {"unique_id": 5, "gender": "f", "postcode": ["2000"]},
            {
                "unique_id": 6,
                "gender": "m",
                "postcode": ["2617", "2612", "2000"],
            },
        ]
    )
    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            {
                "blocking_rule": (
                    "l.gender = r.gender AND l.postcode = r.postcode"
                ),
                "arrays_to_explode": ["postcode"],
            },
            "l.gender = r.gender",
        ],
        "comparisons": [cl.ArrayIntersectAtSizes("postcode", [1])],
    }
    linker = test_helpers["duckdb"].linker_with_registration(
        [data_l, data_r],
        settings,
    )
    df_predict = linker.inference.predict(warning_mode="never")

    importance = linker.blocking_analysis.blocking_rule_importance(df_predict)

    assert [
        (
            record["comparison_count"],
            record["overlapping_comparison_count"],
            record["marginal_comparison_count"],
            record["is_redundant"],
        )
        for record in importance
    ] == [
        (3, 3, 0, True),
        (5, 3, 2, False),
    ]

    marginal_match_probability = sum(
        record["match_probability"]
        for record in df_predict.as_record_list()
        if (int(record["unique_id_l"]), int(record["unique_id_r"]))
        in {(1, 4), (3, 5)}
    )
    assert importance[1]["estimated_marginal_match_count"] == pytest.approx(
        marginal_match_probability
    )
