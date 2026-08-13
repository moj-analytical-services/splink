from collections import Counter

import pytest

import splink.comparison_library as cl
from splink import SettingsCreator, block_on
from tests.decorator import mark_with_dialects_including


data = [
    {"unique_id": 1, "first_name": "Alice", "surname": "Smith", "dob": "1990"},
    {"unique_id": 2, "first_name": "Alice", "surname": "Jones", "dob": "1991"},
    {"unique_id": 3, "first_name": "Bob", "surname": "Smith", "dob": "1992"},
    {"unique_id": 4, "first_name": "Carol", "surname": "Green", "dob": "1993"},
]


def _linker_and_predictions(test_helpers):
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("surname"),
        ],
    )
    linker = test_helpers["duckdb"].linker_with_registration([data], settings)
    predictions = linker.inference.predict(warning_mode="never")
    return linker, predictions


@mark_with_dialects_including("duckdb")
def test_blocking_rule_performance_chart(test_helpers):
    linker, predictions = _linker_and_predictions(test_helpers)

    chart = linker.visualisations.blocking_rule_performance_chart(
        df_predict=predictions
    )
    records = chart.chart_data

    assert len(records) == 2 * 41
    assert {record["match_key"] for record in records} == {0, 1}
    assert {record["mw_threshold"] for record in records} == {
        threshold / 4 for threshold in range(41)
    }
    assert {record["total_edges"] for record in records} == {1}
    expected_matches_at_zero = Counter(
        int(record["match_key"])
        for record in predictions.as_record_list()
        if record["match_weight"] >= 0
    )
    actual_matches_at_zero = {
        record["match_key"]: record["matches"]
        for record in records
        if record["mw_threshold"] == 0
    }
    assert actual_matches_at_zero == {
        match_key: expected_matches_at_zero[match_key] for match_key in (0, 1)
    }
    assert all(
        record["non_matches"] == record["total_edges"] - record["matches"]
        for record in records
    )

    chart_dict = chart.chart_dict
    assert len(chart_dict["vconcat"]) == 1
    assert chart_dict["title"]["subtitle"] == (
        "Number of matches and non-matches returned by each blocking rule"
    )
    blocking_rule_panel = chart_dict["vconcat"][0]
    non_match_panel = blocking_rule_panel["hconcat"][0]
    match_panel = blocking_rule_panel["hconcat"][2]
    assert non_match_panel["encoding"]["x"]["field"] == "non_matches_for_chart"
    assert match_panel["encoding"]["x"]["field"] == "matches_for_chart"
    assert non_match_panel["layer"][0]["encoding"]["x2"] == {"datum": 1}
    assert match_panel["layer"][0]["encoding"]["x2"] == {"datum": 1}
    assert "x2" not in non_match_panel["encoding"]
    assert "x2" not in match_panel["encoding"]
    chart.altair_chart.to_dict()


@mark_with_dialects_including("duckdb")
def test_blocking_rule_performance_chart_with_missing_edges(test_helpers):
    linker, predictions = _linker_and_predictions(test_helpers)
    predictions_with_missing = linker.misc.query_sql(
        f"""
        SELECT * FROM {predictions.physical_name}
        UNION ALL
        SELECT * REPLACE ('-1' AS match_key)
        FROM (SELECT * FROM {predictions.physical_name} LIMIT 1)
        """
    )

    chart = linker.visualisations.blocking_rule_performance_chart(
        predictions_with_missing,
        missing_edges=True,
    )
    missed_records = [
        record for record in chart.chart_data if record["match_key"] == -1
    ]

    assert len(missed_records) == 41
    assert {record["total_edges"] for record in missed_records} == {1}
    assert len(chart.chart_dict["vconcat"]) == 2
    chart.altair_chart.to_dict()

    chart_with_supplied_count = (
        linker.visualisations.blocking_rule_performance_chart(
            predictions_with_missing,
            missing_edges=7,
        )
    )
    supplied_count_records = [
        record
        for record in chart_with_supplied_count.chart_data
        if record["match_key"] == -1
    ]
    assert {record["total_edges"] for record in supplied_count_records} == {7}


@mark_with_dialects_including("duckdb")
def test_blocking_rule_performance_chart_validates_missing_edges(test_helpers):
    linker, predictions = _linker_and_predictions(test_helpers)

    with pytest.raises(ValueError, match="non-negative"):
        linker.visualisations.blocking_rule_performance_chart(
            predictions,
            missing_edges=-1,
        )

    with pytest.raises(TypeError, match="bool or a non-negative integer"):
        linker.visualisations.blocking_rule_performance_chart(
            predictions,
            missing_edges=1.5,
        )


@mark_with_dialects_including("duckdb")
def test_blocking_rule_performance_chart_validates_predictions(test_helpers):
    linker, predictions = _linker_and_predictions(test_helpers)

    unexpected_match_key = linker.misc.query_sql(
        f"""
        SELECT * REPLACE ('2' AS match_key)
        FROM {predictions.physical_name}
        LIMIT 1
        """
    )
    with pytest.raises(ValueError, match=r"not present.*\[2\]"):
        linker.visualisations.blocking_rule_performance_chart(unexpected_match_key)

    duplicate_high_scoring_edges = linker.misc.query_sql(
        f"""
        SELECT * REPLACE (10.0 AS match_weight)
        FROM {predictions.physical_name}
        WHERE CAST(match_key AS INTEGER) = 0
        UNION ALL
        SELECT * REPLACE (10.0 AS match_weight)
        FROM {predictions.physical_name}
        WHERE CAST(match_key AS INTEGER) = 0
        """
    )
    with pytest.raises(ValueError, match=r"2 edges.*total edge count is 1"):
        linker.visualisations.blocking_rule_performance_chart(
            duplicate_high_scoring_edges
        )
