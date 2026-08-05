import pytest

import splink.internals.comparison_library as cl
from splink import Linker, SettingsCreator
from splink.internals.blocking_rule_library import block_on
from splink.internals.exceptions import SplinkException

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding("sqlite")
def test_predict_between_never_within_a_role(test_helpers, dialect):
    # The two left records (1, 2) share first_name "alice" and so *would* block
    # together within a single collection. predict_between must never pair them:
    # every pair must be left x right.
    helper = test_helpers[dialect]
    db_api = helper.db_api()

    left = [
        {"unique_id": 1, "first_name": "alice"},
        {"unique_id": 2, "first_name": "alice"},
    ]
    right = [
        {"unique_id": 3, "first_name": "alice"},
        {"unique_id": 4, "first_name": "bob"},
    ]
    left_sdf = db_api.register(left, table_name="left_role")
    right_sdf = db_api.register(right, table_name="right_role")

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[cl.ExactMatch("first_name")],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
    )
    linker = Linker(left_sdf, settings)

    df_between = linker.inference.predict_between(
        left_sdf, right_sdf, warning_mode="never"
    )
    actual = {(r["unique_id_l"], r["unique_id_r"]) for r in df_between.as_record_list()}
    # left alice {1, 2} x right alice {3}; bob (4) does not block.
    # (1, 2) is absent -> the within-left pair is never generated.
    assert actual == {(1, 3), (2, 3)}


@mark_with_dialects_excluding("sqlite")
def test_predict_between_link_only_source_filter(test_helpers, dialect):
    # Both roles span the same three source datasets a/b/c, all blocking together.
    # link_only must drop same-source pairs; link_and_dedupe must keep them.
    helper = test_helpers[dialect]
    db_api = helper.db_api()

    def _reg(uid, sds, table_name):
        return db_api.register(
            [{"unique_id": uid, "first_name": "alice"}],
            dataset_display_name=sds,
            table_name=table_name,
        )

    left = [_reg(1, "a", "la"), _reg(2, "b", "lb"), _reg(3, "c", "lc")]
    right = [_reg(4, "a", "ra"), _reg(5, "b", "rb"), _reg(6, "c", "rc")]

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[cl.ExactMatch("first_name")],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
    )
    linker = Linker(left, settings)

    def _pairs(sdf):
        return {
            (
                (r["source_dataset_l"], r["unique_id_l"]),
                (r["source_dataset_r"], r["unique_id_r"]),
            )
            for r in sdf.as_record_list()
        }

    df_link_only = linker.inference.predict_between(left, right, warning_mode="never")
    df_keep_all = linker.inference.predict_between(
        left, right, link_type="link_and_dedupe", warning_mode="never"
    )

    # link_only: every left x right pair except the three same-source ones.
    assert _pairs(df_link_only) == {
        (("a", 1), ("b", 5)),
        (("a", 1), ("c", 6)),
        (("b", 2), ("a", 4)),
        (("b", 2), ("c", 6)),
        (("c", 3), ("a", 4)),
        (("c", 3), ("b", 5)),
    }
    # link_and_dedupe: the full 3 x 3 left x right grid, same-source pairs kept.
    keep_all = _pairs(df_keep_all)
    assert len(keep_all) == 9
    same_source = {(("a", 1), ("a", 4)), (("b", 2), ("b", 5)), (("c", 3), ("c", 6))}
    assert same_source <= keep_all


@mark_with_dialects_excluding("sqlite", "postgres")
def test_predict_between_exploding_blocking_rule(test_helpers, dialect):
    import pyarrow as pa

    helper = test_helpers[dialect]
    db_api = helper.db_api()
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
    left_sdf = db_api.register(
        data_l, dataset_display_name="left_role", table_name="between_left_arr"
    )
    right_sdf = db_api.register(
        data_r, dataset_display_name="right_role", table_name="between_right_arr"
    )
    linker = Linker([left_sdf, right_sdf], settings)

    df_between = linker.inference.predict_between(
        left_sdf, right_sdf, warning_mode="never"
    )
    actual = {
        (r["unique_id_l"], r["unique_id_r"], r["match_key"])
        for r in df_between.as_record_list()
    }
    expected = {(1, 6, "0"), (2, 4, "0"), (2, 6, "0"), (1, 4, "1"), (3, 5, "1")}
    assert actual == expected


@mark_with_dialects_excluding("sqlite")
def test_predict_between_missing_tf_raises(test_helpers, dialect):
    # A term-frequency adjustment is requested but no tf table is registered:
    # predict_between must refuse rather than derive tf from the supplied subset.
    helper = test_helpers[dialect]
    db_api = helper.db_api()

    left = [{"unique_id": 1, "first_name": "alice"}]
    right = [{"unique_id": 2, "first_name": "alice"}]
    left_sdf = db_api.register(left, table_name="left_tf")
    right_sdf = db_api.register(right, table_name="right_tf")

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name").configure(term_frequency_adjustments=True)
        ],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
    )
    linker = Linker(left_sdf, settings)

    with pytest.raises(SplinkException):
        linker.inference.predict_between(left_sdf, right_sdf, warning_mode="never")
