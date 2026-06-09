import pytest

import splink.internals.comparison_library as cl
from splink import SettingsCreator
from splink.internals.blocking_rule_library import block_on
from splink.internals.exceptions import SplinkException

from .decorator import mark_with_dialects_excluding


def _dedupe_settings(tf=False):
    return SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name").configure(term_frequency_adjustments=tf),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city"),
            cl.ExactMatch("email"),
        ],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
        retain_intermediate_calculation_columns=True,
        retain_matching_columns=True,
    )


def _train(linker):
    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name"), block_on("surname")], recall=0.7
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5, seed=1)


def _pairs(sdf):
    return {(r["unique_id_l"], r["unique_id_r"]) for r in sdf.as_record_dict()}


@mark_with_dialects_excluding("sqlite")
def test_predict_between_role_split(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _dedupe_settings())
    _train(linker)

    records = fake_1000.to_pylist()
    half = len(records) // 2
    left_records = records[:half]
    right_records = records[half:]
    left_ids = {r["unique_id"] for r in left_records}
    right_ids = {r["unique_id"] for r in right_records}

    left_sdf = linker._db_api.register(left_records, "between_left")
    right_sdf = linker._db_api.register(right_records, "between_right")

    df_between = linker.inference.predict_between(left_sdf, right_sdf)
    rows = df_between.as_record_dict()
    assert len(rows) > 0
    for r in rows:
        # Left record always drawn from `left`, right record always from `right`
        assert r["unique_id_l"] in left_ids
        assert r["unique_id_r"] in right_ids


@mark_with_dialects_excluding("sqlite")
def test_predict_between_link_only_source_filter(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]

    records = fake_1000.to_pylist()[:200]
    for i, r in enumerate(records):
        r["source_dataset"] = "a" if i % 2 == 0 else "b"

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

    import pyarrow as pa

    combined = pa.Table.from_pylist(records)
    linker = helper.linker_with_registration(combined, settings)
    _train(linker)

    # Use the same record collection as both roles so that same-source candidate
    # pairs are guaranteed to exist (and so can be shown to be filtered out).
    left_sdf = linker._db_api.register(records, "between_left_src")
    right_sdf = linker._db_api.register(records, "between_right_src")

    df_link_only = linker.inference.predict_between(left_sdf, right_sdf)
    df_keep_all = linker.inference.predict_between(
        left_sdf, right_sdf, link_type="link_and_dedupe"
    )

    link_only_rows = df_link_only.as_record_dict()
    keep_all_rows = df_keep_all.as_record_dict()

    assert len(link_only_rows) > 0
    # link_only must only return cross-source pairs
    for r in link_only_rows:
        assert r["source_dataset_l"] != r["source_dataset_r"]

    # link_and_dedupe keeps same-source pairs that link_only filters out
    assert any(r["source_dataset_l"] == r["source_dataset_r"] for r in keep_all_rows)
    assert len(keep_all_rows) > len(link_only_rows)


@mark_with_dialects_excluding("sqlite")
def test_predict_between_missing_tf_raises(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _dedupe_settings(tf=True))
    _train(linker)
    linker.table_management.invalidate_cache()

    records = fake_1000.to_pylist()
    half = len(records) // 2
    left_sdf = linker._db_api.register(records[:half], "between_left_tf")
    right_sdf = linker._db_api.register(records[half:], "between_right_tf")

    with pytest.raises(SplinkException):
        linker.inference.predict_between(left_sdf, right_sdf)
