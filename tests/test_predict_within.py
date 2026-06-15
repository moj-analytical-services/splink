import pytest

import splink.internals.comparison_library as cl
from splink import SettingsCreator
from splink.internals.blocking_rule_library import block_on
from splink.internals.exceptions import SplinkException

from .decorator import mark_with_dialects_excluding


def _dedupe_settings(tf=False, blocking_rules=None):
    if blocking_rules is None:
        blocking_rules = [block_on("first_name"), block_on("surname")]
    return SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name").configure(term_frequency_adjustments=tf),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city"),
            cl.ExactMatch("email"),
        ],
        blocking_rules_to_generate_predictions=blocking_rules,
        retain_intermediate_calculation_columns=True,
        retain_matching_columns=True,
    )


def _train(linker):
    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name"), block_on("surname")], recall=0.7
    )
    estimate_u_kwargs = {"max_pairs": 1e5}
    if linker._sql_dialect_str != "postgres":
        estimate_u_kwargs["seed"] = 1
    linker.training.estimate_u_using_random_sampling(**estimate_u_kwargs)


def _pairs_with_weight(sdf):
    rows = sdf.as_record_dict()
    return {
        (r["unique_id_l"], r["unique_id_r"], round(r["match_weight"], 8)) for r in rows
    }


@mark_with_dialects_excluding("sqlite")
def test_predict_within_matches_predict(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _dedupe_settings())
    _train(linker)

    df_predict = linker.inference.predict()
    df_within = linker.inference.predict_within(
        list(linker._input_tables_dict.values())
    )

    assert _pairs_with_weight(df_predict) == _pairs_with_weight(df_within)


@mark_with_dialects_excluding("sqlite")
def test_predict_within_blocking_rule_override(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _dedupe_settings())
    _train(linker)

    df_within = linker.inference.predict_within(
        list(linker._input_tables_dict.values()),
        blocking_rules_to_generate_predictions=[block_on("dob")],
    )
    rows = df_within.as_record_dict()
    assert len(rows) > 0
    # Every returned pair must satisfy the supplied blocking rule
    for r in rows:
        assert r["dob_l"] == r["dob_r"]


@mark_with_dialects_excluding("sqlite")
def test_predict_within_link_only_cross_pairs_only(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    records = fake_1000.to_pylist()
    third = len(records) // 3
    parts = [records[:third], records[third : 2 * third], records[2 * third :]]

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city"),
            cl.ExactMatch("email"),
        ],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
        retain_intermediate_calculation_columns=True,
        retain_matching_columns=True,
    )

    linker = helper.linker_with_registration(
        parts, settings, input_table_aliases=["a", "b", "c"]
    )
    _train(linker)

    sdfs = list(linker._input_tables_dict.values())
    df_within = linker.inference.predict_within(sdfs)
    rows = df_within.as_record_dict()
    assert len(rows) > 0
    # link_only must never return within-dataset pairs
    for r in rows:
        assert r["source_dataset_l"] != r["source_dataset_r"]


@mark_with_dialects_excluding("sqlite")
def test_predict_within_missing_tf_raises(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _dedupe_settings(tf=True))
    _train(linker)
    # Term-frequency tables are computed and cached during training; clear them so
    # that predict_within has no registered tf information to use.
    linker.table_management.invalidate_cache()

    with pytest.raises(SplinkException):
        linker.inference.predict_within(list(linker._input_tables_dict.values()))


@mark_with_dialects_excluding("sqlite")
def test_predict_within_registered_tf_succeeds(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _dedupe_settings(tf=True))
    _train(linker)
    # Ensure the required tf table is registered
    linker.table_management.compute_tf_table("first_name")

    df_within = linker.inference.predict_within(
        list(linker._input_tables_dict.values())
    )
    assert len(df_within.as_record_dict()) > 0


@mark_with_dialects_excluding("sqlite", "postgres")
def test_predict_within_exploding_blocking_rule(test_helpers, dialect):
    import pyarrow as pa

    helper = test_helpers[dialect]
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
            {"unique_id": 6, "gender": "m", "postcode": ["2617", "2612", "2000"]},
        ]
    )
    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            {
                "blocking_rule": "l.gender = r.gender and l.postcode = r.postcode",
                "arrays_to_explode": ["postcode"],
            },
            "l.gender = r.gender",
        ],
        "comparisons": [cl.ArrayIntersectAtSizes("postcode", [1])],
    }
    linker = helper.linker_with_registration([data_l, data_r], settings)

    # predict_within with an exploding blocking rule must match predict()
    df_predict = linker.inference.predict()
    df_within = linker.inference.predict_within(
        list(linker._input_tables_dict.values())
    )

    def _triples(sdf):
        return {
            (r["unique_id_l"], r["unique_id_r"], r["match_key"])
            for r in sdf.as_record_dict()
        }

    expected = {(1, 6, "0"), (2, 4, "0"), (2, 6, "0"), (1, 4, "1"), (3, 5, "1")}
    assert _triples(df_predict) == expected
    assert _triples(df_within) == expected
