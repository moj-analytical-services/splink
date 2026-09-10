import splink.comparison_library as cl
from splink import SettingsCreator, block_on
from splink.internals.blocking_rule_performance import (
    blocking_rule_performance_data,
)
from tests.decorator import mark_with_dialects_including


@mark_with_dialects_including("duckdb")
def test_blocking_rule_performance_data(test_helpers, monkeypatch):
    sentinel_input_data = [
        {
            "unique_id": 1,
            "first_name": "__unused__",
            "surname": "__unused__",
        }
    ]
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("surname"),
        ],
    )
    linker = test_helpers["duckdb"].linker_with_registration(
        [sentinel_input_data], settings
    )
    monkeypatch.setattr(
        linker.blocking_analysis,
        "count_comparisons_from_blocking_rules",
        lambda **kwargs: [
            {
                "match_key": 0,
                "marginal_comparison_count": 6,
                "blocking_rule": "first_name rule",
            },
            {
                "match_key": 1,
                "marginal_comparison_count": 4,
                "blocking_rule": "surname rule",
            },
        ],
    )
    df_predict = linker._db_api.register(
        [
            {"match_key": 0, "match_weight": 2.01},
            {"match_key": 0, "match_weight": 2.24},
            {"match_key": 0, "match_weight": 2.25},
            {"match_key": 0, "match_weight": 3.0},
            {"match_key": 0, "match_weight": 5.0},
            {"match_key": 0, "match_weight": 10.0},
            {"match_key": 1, "match_weight": 2.01},
            {"match_key": 1, "match_weight": 2.5},
            {"match_key": 1, "match_weight": 5.0},
            {"match_key": 1, "match_weight": 9.0},
        ]
    )

    records = blocking_rule_performance_data(linker, df_predict)
    records_by_key_and_threshold = {
        (record["match_key"], record["mw_threshold"]): (
            record["total_edges"],
            record["matches"],
            record["non_matches"],
        )
        for record in records
    }

    assert len(records) == 64
    assert {
        key: records_by_key_and_threshold[key]
        for key in ((0, 2.25), (0, 5.0), (0, 10.0))
    } == {
        (0, 2.25): (6, 4, 2),
        (0, 5.0): (6, 2, 4),
        (0, 10.0): (6, 1, 5),
    }
    assert {
        key: records_by_key_and_threshold[key]
        for key in ((1, 2.25), (1, 5.0), (1, 10.0))
    } == {
        (1, 2.25): (4, 3, 1),
        (1, 5.0): (4, 2, 2),
        (1, 10.0): (4, 0, 4),
    }
