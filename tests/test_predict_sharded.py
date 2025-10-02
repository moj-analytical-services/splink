import logging

import pandas as pd
import pandas.testing as pdt
import pytest

import splink.internals.comparison_library as cl
from splink import block_on

from .decorator import mark_with_dialects_including


def _dedupe_settings():
    return {
        "link_type": "dedupe_only",
        "unique_id_column_name": "unique_id",
        "probability_two_random_records_match": 0.001,
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
        ],
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
        ],
        "retain_matching_columns": True,
    }


def _link_settings():
    settings = _dedupe_settings()
    settings["link_type"] = "link_only"
    return settings


def _multi_rule_settings():
    settings = _dedupe_settings()
    settings["blocking_rules_to_generate_predictions"] = [
        block_on("first_name"),
        block_on("surname", salting_partitions=2),
    ]
    return settings


def _sort_predictions(df: pd.DataFrame) -> pd.DataFrame:
    if {"unique_id_l", "unique_id_r"}.issubset(df.columns):
        return df.sort_values(["unique_id_l", "unique_id_r"]).reset_index(drop=True)
    return df.reset_index(drop=True)


def _make_people_df(size: int, offset: int = 0) -> pd.DataFrame:
    records = [
        {
            "unique_id": offset + i,
            "first_name": f"first_{i % 5}",
            "surname": f"surname_{i % 7}",
        }
        for i in range(size)
    ]
    return pd.DataFrame(records)


@mark_with_dialects_including("duckdb", "sqlite", pass_dialect=True)
@pytest.mark.parametrize("shard_side", ["left", "auto"])
def test_predict_sharded_matches_predict_dedupe(dialect, test_helpers, shard_side):
    helper = test_helpers[dialect]
    pandas_df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv").head(50)

    full_input = helper.convert_frame(pandas_df.copy())
    linker_full = helper.Linker(full_input, _dedupe_settings(), **helper.extra_linker_args())
    predictions_full = _sort_predictions(
        linker_full.inference.predict().as_pandas_dataframe()
    )

    sharded_input = helper.convert_frame(pandas_df.copy())
    linker_sharded = helper.Linker(
        sharded_input,
        _dedupe_settings(),
        **helper.extra_linker_args(),
    )
    predictions_sharded = _sort_predictions(
        linker_sharded.inference.predict_sharded(
            num_shards=3, shard_side=shard_side
        ).as_pandas_dataframe()
    )

    pdt.assert_frame_equal(predictions_full, predictions_sharded)
    assert all("shard" not in col.lower() for col in predictions_sharded.columns)

    for df in linker_sharded._input_tables_dict.values():
        assert all(col.unquote().name.lower() != "shard" for col in df.columns)


@mark_with_dialects_including("duckdb", "sqlite", pass_dialect=True)
def test_predict_sharded_matches_predict_link(dialect, test_helpers, caplog):
    helper = test_helpers[dialect]

    left_df = _make_people_df(50)
    right_df = _make_people_df(80, offset=1_000)

    left_full = helper.convert_frame(left_df.copy())
    right_full = helper.convert_frame(right_df.copy())
    linker_full = helper.Linker(
        [left_full, right_full],
        _link_settings(),
        **helper.extra_linker_args(),
    )
    predictions_full = _sort_predictions(
        linker_full.inference.predict().as_pandas_dataframe()
    )

    for shard_side in ["left", "right"]:
        linker_sharded = helper.Linker(
            [helper.convert_frame(left_df.copy()), helper.convert_frame(right_df.copy())],
            _link_settings(),
            **helper.extra_linker_args(),
        )
        predictions_sharded = _sort_predictions(
            linker_sharded.inference.predict_sharded(
                num_shards=3, shard_side=shard_side
            ).as_pandas_dataframe()
        )
        pdt.assert_frame_equal(predictions_full, predictions_sharded)
        assert all("shard" not in col.lower() for col in predictions_sharded.columns)

    caplog.clear()
    linker_auto = helper.Linker(
        [helper.convert_frame(left_df.copy()), helper.convert_frame(right_df.copy())],
        _link_settings(),
        **helper.extra_linker_args(),
    )
    with caplog.at_level(
        logging.INFO, logger="splink.internals.linker_components.inference"
    ):
        predictions_auto = _sort_predictions(
            linker_auto.inference.predict_sharded(
                num_shards=3, shard_side="auto"
            ).as_pandas_dataframe()
        )

    pdt.assert_frame_equal(predictions_full, predictions_auto)
    assert all("shard" not in col.lower() for col in predictions_auto.columns)

    messages = [
        record.getMessage()
        for record in caplog.records
        if "predict_sharded run" in record.getMessage()
    ]
    assert messages
    assert any("shard_side='right'" in message for message in messages)


@mark_with_dialects_including("duckdb", "sqlite", pass_dialect=True)
def test_predict_sharded_auto_tie_break_prefers_left(dialect, test_helpers, caplog):
    helper = test_helpers[dialect]

    left_df = _make_people_df(100)
    right_df = _make_people_df(105, offset=5_000)

    linker_full = helper.Linker(
        [helper.convert_frame(left_df.copy()), helper.convert_frame(right_df.copy())],
        _link_settings(),
        **helper.extra_linker_args(),
    )
    predictions_full = _sort_predictions(
        linker_full.inference.predict().as_pandas_dataframe()
    )

    caplog.clear()
    linker_auto = helper.Linker(
        [helper.convert_frame(left_df.copy()), helper.convert_frame(right_df.copy())],
        _link_settings(),
        **helper.extra_linker_args(),
    )
    with caplog.at_level(
        logging.INFO, logger="splink.internals.linker_components.inference"
    ):
        predictions_auto = _sort_predictions(
            linker_auto.inference.predict_sharded(
                num_shards=4, shard_side="auto"
            ).as_pandas_dataframe()
        )

    pdt.assert_frame_equal(predictions_full, predictions_auto)
    assert all("shard" not in col.lower() for col in predictions_auto.columns)

    messages = [
        record.getMessage()
        for record in caplog.records
        if "predict_sharded run" in record.getMessage()
    ]
    assert messages
    assert any("shard_side='left'" in message for message in messages)


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_predict_sharded_validates_shard_count(dialect, test_helpers):
    helper = test_helpers[dialect]
    pandas_df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv").head(10)

    linker = helper.Linker(
        helper.convert_frame(pandas_df),
        _dedupe_settings(),
        **helper.extra_linker_args(),
    )

    with pytest.raises(ValueError):
        linker.inference.predict_sharded(1)

    with pytest.raises(ValueError):
        linker.inference.predict_sharded(51)

    with pytest.raises(ValueError):
        linker.inference.predict_sharded(3.5)


@mark_with_dialects_including("duckdb", "sqlite", pass_dialect=True)
def test_predict_sharded_rejects_existing_shard_column(dialect, test_helpers):
    helper = test_helpers[dialect]

    pandas_df = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "Alice", "surname": "Smith", "shard": 1},
            {"unique_id": 2, "first_name": "Bob", "surname": "Jones", "shard": 2},
        ]
    )

    linker = helper.Linker(
        helper.convert_frame(pandas_df),
        _dedupe_settings(),
        **helper.extra_linker_args(),
    )

    with pytest.raises(ValueError, match="column named 'shard'"):
        linker.inference.predict_sharded(num_shards=2)


@mark_with_dialects_including("duckdb", "sqlite", pass_dialect=True)
def test_predict_sharded_right_requires_two_tables(dialect, test_helpers):
    helper = test_helpers[dialect]
    pandas_df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv").head(10)

    linker = helper.Linker(
        helper.convert_frame(pandas_df),
        _dedupe_settings(),
        **helper.extra_linker_args(),
    )

    with pytest.raises(ValueError, match="Cannot shard the right side"):
        linker.inference.predict_sharded(num_shards=2, shard_side="right")


@mark_with_dialects_including("duckdb", "sqlite", pass_dialect=True)
def test_predict_sharded_with_multiple_blocking_rules_matches_predict(
    dialect, test_helpers
):
    helper = test_helpers[dialect]
    pandas_df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv").head(80)

    full_linker = helper.Linker(
        helper.convert_frame(pandas_df.copy()),
        _multi_rule_settings(),
        **helper.extra_linker_args(),
    )
    predictions_full = _sort_predictions(
        full_linker.inference.predict().as_pandas_dataframe()
    )

    sharded_linker = helper.Linker(
        helper.convert_frame(pandas_df.copy()),
        _multi_rule_settings(),
        **helper.extra_linker_args(),
    )
    predictions_sharded = _sort_predictions(
        sharded_linker.inference.predict_sharded(
            num_shards=3, shard_side="auto"
        ).as_pandas_dataframe()
    )

    pdt.assert_frame_equal(predictions_full, predictions_sharded)
    assert all("shard" not in col.lower() for col in predictions_sharded.columns)

    if {"unique_id_l", "unique_id_r"}.issubset(predictions_sharded.columns):
        pairs = predictions_sharded[["unique_id_l", "unique_id_r"]]
        assert not pairs.duplicated().any()

