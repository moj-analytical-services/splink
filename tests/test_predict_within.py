import pytest

import splink.internals.comparison_library as cl
from splink import SettingsCreator
from splink.internals.blocking_rule_library import block_on
from splink.internals.exceptions import SplinkException

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding("sqlite")
def test_predict_within_matches_predict(test_helpers, dialect):
    import pyarrow as pa

    # predict_within over the linker's own inputs must be identical to predict(),
    # pairs *and* match weights.
    helper = test_helpers[dialect]
    records = pa.Table.from_pylist(
        [
            {"unique_id": 1, "first_name": "alice"},
            {"unique_id": 2, "first_name": "alice"},
            {"unique_id": 3, "first_name": "alice"},
            {"unique_id": 4, "first_name": "bob"},
            {"unique_id": 5, "first_name": "bob"},
        ]
    )
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[cl.ExactMatch("first_name")],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
    )
    linker = helper.linker_with_registration(records, settings)

    def _pairs_with_weight(sdf):
        return {
            (r["unique_id_l"], r["unique_id_r"], round(r["match_weight"], 8))
            for r in sdf.as_record_dict()
        }

    df_predict = linker.inference.predict()
    df_within = linker.inference.predict_within(
        list(linker._input_tables_dict.values()), warning_mode="never"
    )
    assert _pairs_with_weight(df_predict) == _pairs_with_weight(df_within)


@mark_with_dialects_excluding("sqlite")
def test_predict_within_link_only_cross_source_only(test_helpers, dialect):
    # Three source datasets, every record blocks together (all "alice").
    # Source "a" deliberately contains two records (1, 2) so a within-source
    # candidate pair exists and can be shown to be filtered out by link_only.
    helper = test_helpers[dialect]
    part_a = [
        {"unique_id": 1, "first_name": "alice"},
        {"unique_id": 2, "first_name": "alice"},
    ]
    part_b = [{"unique_id": 3, "first_name": "alice"}]
    part_c = [{"unique_id": 4, "first_name": "alice"}]

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[cl.ExactMatch("first_name")],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
    )
    linker = helper.linker_with_registration(
        [part_a, part_b, part_c], settings, input_table_aliases=["a", "b", "c"]
    )

    df_within = linker.inference.predict_within(
        list(linker._input_tables_dict.values()), warning_mode="never"
    )
    actual = {
        frozenset(
            (
                (r["source_dataset_l"], r["unique_id_l"]),
                (r["source_dataset_r"], r["unique_id_r"]),
            )
        )
        for r in df_within.as_record_dict()
    }
    # Cross-source pairs only; the same-source pair (a1, a2) must be absent.
    expected = {
        frozenset((("a", 1), ("b", 3))),
        frozenset((("a", 1), ("c", 4))),
        frozenset((("a", 2), ("b", 3))),
        frozenset((("a", 2), ("c", 4))),
        frozenset((("b", 3), ("c", 4))),
    }
    assert actual == expected


@mark_with_dialects_excluding("sqlite", "postgres")
def test_predict_within_exploding_blocking_rule(test_helpers, dialect):
    import pyarrow as pa

    helper = test_helpers[dialect]
    data = pa.Table.from_pylist(
        [
            {"unique_id": 1, "gender": "m", "postcode": ["2612", "2000"]},
            {"unique_id": 2, "gender": "m", "postcode": ["2612", "2617"]},
            {"unique_id": 3, "gender": "f", "postcode": ["2617"]},
            {"unique_id": 4, "gender": "m", "postcode": ["2617", "2600"]},
            {"unique_id": 5, "gender": "f", "postcode": ["2000"]},
            {"unique_id": 6, "gender": "m", "postcode": ["2617", "2612", "2000"]},
        ]
    )
    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            {
                "blocking_rule": "l.gender = r.gender and l.postcode = r.postcode",
                "arrays_to_explode": ["postcode"],
            },
            "l.gender = r.gender",
        ],
        "comparisons": [cl.ArrayIntersectAtSizes("postcode", [1])],
    }
    linker = helper.linker_with_registration(data, settings)

    # predict_within over the linker's own input must match predict().
    df_predict = linker.inference.predict()
    df_within = linker.inference.predict_within(
        list(linker._input_tables_dict.values()), warning_mode="never"
    )

    def _triples(sdf):
        return {
            (r["unique_id_l"], r["unique_id_r"], r["match_key"])
            for r in sdf.as_record_dict()
        }

    assert _triples(df_within) == _triples(df_predict)
    assert len(_triples(df_within)) > 0


@mark_with_dialects_excluding("sqlite")
def test_predict_within_missing_tf_raises(test_helpers, dialect):
    import pyarrow as pa

    # A term-frequency adjustment is requested but no tf table is registered and
    # no hardcoded tf_* column is supplied: predict_within must refuse rather than
    # silently derive tf from the (possibly unrepresentative) supplied records.
    helper = test_helpers[dialect]
    records = pa.Table.from_pylist(
        [
            {"unique_id": 1, "first_name": "alice"},
            {"unique_id": 2, "first_name": "alice"},
        ]
    )
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name").configure(term_frequency_adjustments=True)
        ],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
    )
    linker = helper.linker_with_registration(records, settings)

    with pytest.raises(SplinkException):
        linker.inference.predict_within(
            list(linker._input_tables_dict.values()), warning_mode="never"
        )
