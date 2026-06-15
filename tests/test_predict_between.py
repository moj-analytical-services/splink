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


def _train(linker, dialect):
    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name"), block_on("surname")], recall=0.7
    )
    estimate_u_kwargs = {"max_pairs": 1e5}
    if dialect != "postgres":
        estimate_u_kwargs["seed"] = 1
    linker.training.estimate_u_using_random_sampling(**estimate_u_kwargs)


def _pairs(sdf):
    return {(r["unique_id_l"], r["unique_id_r"]) for r in sdf.as_record_dict()}


@mark_with_dialects_excluding("sqlite")
def test_predict_between_role_split(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _dedupe_settings())
    _train(linker, dialect)

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
    _train(linker, dialect)

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
    _train(linker, dialect)
    linker.table_management.invalidate_cache()

    records = fake_1000.to_pylist()
    half = len(records) // 2
    left_sdf = linker._db_api.register(records[:half], "between_left_tf")
    right_sdf = linker._db_api.register(records[half:], "between_right_tf")

    with pytest.raises(SplinkException):
        linker.inference.predict_between(left_sdf, right_sdf)


@mark_with_dialects_excluding("sqlite")
def test_predict_between_blocks_before_tf_join(
    test_helpers, monkeypatch, dialect, fake_1000
):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _dedupe_settings(tf=True))
    linker.table_management.compute_tf_table("first_name")

    records = fake_1000.to_pylist()
    half = len(records) // 2
    left_sdf = linker._db_api.register(records[:half], "between_left_tf_block")
    right_sdf = linker._db_api.register(records[half:], "between_right_tf_block")

    executed_pipelines = []
    original_sql_pipeline_to_splink_dataframe = (
        linker._db_api.sql_pipeline_to_splink_dataframe
    )

    def capture_pipeline(pipeline):
        executed_pipelines.append(pipeline)
        return original_sql_pipeline_to_splink_dataframe(pipeline)

    monkeypatch.setattr(
        linker._db_api, "sql_pipeline_to_splink_dataframe", capture_pipeline
    )

    linker.inference.predict_between(left_sdf, right_sdf, warning_mode="never")

    blocking_pipeline = next(
        p
        for p in executed_pipelines
        if p.queue[-1].output_table_name == "__splink__blocked_id_pairs"
    )
    blocking_sql = "\n".join(cte.sql for cte in blocking_pipeline.queue)
    assert "__splink__df_concat_left" in blocking_sql
    assert "__splink__df_concat_right" in blocking_sql
    assert "__splink__df_concat_with_tf" not in blocking_sql
    assert "__splink__df_tf_" not in blocking_sql

    scoring_pipeline = next(
        p
        for p in executed_pipelines
        if p.queue[-1].output_table_name == "__splink__df_predict"
    )
    scoring_sql = "\n".join(cte.sql for cte in scoring_pipeline.queue)
    assert "__splink__df_concat_with_tf_left" in scoring_sql
    assert "__splink__df_concat_with_tf_right" in scoring_sql
    assert "__splink__df_tf_first_name" in scoring_sql


@mark_with_dialects_excluding("sqlite", "postgres")
def test_predict_between_exploding_blocking_rule(test_helpers, dialect):
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

    # Block left x right (roles) using a mix of an exploding and a standard rule.
    left_sdf = linker._db_api.register(data_l, "between_left_arr")
    right_sdf = linker._db_api.register(data_r, "between_right_arr")

    df_between = linker.inference.predict_between(left_sdf, right_sdf)
    actual = {
        (r["unique_id_l"], r["unique_id_r"], r["match_key"])
        for r in df_between.as_record_dict()
    }
    expected = {(1, 6, "0"), (2, 4, "0"), (2, 6, "0"), (1, 4, "1"), (3, 5, "1")}
    assert actual == expected
