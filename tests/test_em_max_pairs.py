"""End-to-end tests for `max_pairs` based record sampling in EM training.

These duckdb-only tests check that, for each link type, supplying `max_pairs`
to `estimate_parameters_using_expectation_maximisation` actually reduces the
number of blocked pairs fed to EM, landing roughly at the requested target.
"""

from __future__ import annotations

import pytest

import splink.internals.comparison_library as cl
from splink import SettingsCreator, block_on
from splink.internals.blocking import block_using_rules_sqls
from splink.internals.linker import Linker
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import enqueue_df_concat
from tests.decorator import mark_with_dialects_excluding

TRAINING_RULE = "l.surname = r.surname"


def _settings(link_type: str) -> SettingsCreator:
    return SettingsCreator(
        link_type=link_type,
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("city"),
        ],
        blocking_rules_to_generate_predictions=[TRAINING_RULE],
    )


def _make_linker(helper, df, link_type: str) -> Linker:
    if link_type == "dedupe_only":
        return helper.linker_with_registration(df, _settings("dedupe_only"))

    df_a = df.take(list(range(0, df.num_rows, 2)))
    df_b = df.take(list(range(1, df.num_rows, 2)))
    return helper.linker_with_registration([df_a, df_b], _settings(link_type))


def _count_blocked_pairs(linker: Linker, sample_threshold=None, sample_modulus=None):
    """Re-block with the given sample threshold and count the resulting pairs."""
    settings = linker._settings_obj
    pipeline = CTEPipeline()
    enqueue_df_concat(linker, pipeline)
    sqls = block_using_rules_sqls(
        input_tablename_l="__splink__df_concat",
        input_tablename_r="__splink__df_concat",
        blocking_rules=[block_on("surname").get_blocking_rule(linker._sql_dialect_str)],
        link_type=settings._link_type,
        source_dataset_input_column=(
            settings.column_info_settings.source_dataset_input_column
        ),
        unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        sample_threshold=sample_threshold,
        sample_modulus=sample_modulus,
    )
    pipeline.enqueue_list_of_sqls(sqls)
    pipeline.enqueue_sql("select count(*) as c from __splink__blocked_id_pairs", "__c")
    df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    n = int(df.as_record_dict()[0]["c"])
    df.drop_table_from_database_and_remove_from_cache()
    return n


@mark_with_dialects_excluding()
@pytest.mark.parametrize("link_type", ["dedupe_only", "link_only", "link_and_dedupe"])
def test_em_max_pairs_reduces_pair_count(test_helpers, dialect, fake_1000, link_type):
    helper = test_helpers[dialect]
    linker = _make_linker(helper, fake_1000, link_type)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    # How many pairs the training rule generates with no sampling
    unsampled = _count_blocked_pairs(linker)

    target_max_pairs = unsampled // 4
    session = linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("surname"),
        max_pairs=target_max_pairs,
        probe_proportion=0.5,
    )

    info = session._sample_info
    assert info["sampling_applied"] is True, f"{link_type}: expected sampling to apply"

    sampled = _count_blocked_pairs(
        linker,
        sample_threshold=session._sample_threshold,
        sample_modulus=session._sample_modulus,
    )

    # Sampling should bring the pair count down, roughly to the target.
    assert sampled < unsampled, f"{link_type}: sampling did not reduce pairs"
    assert (
        0.4 * target_max_pairs <= sampled <= 1.8 * target_max_pairs
    ), f"{link_type}: sampled {sampled}, target {target_max_pairs}"


@mark_with_dialects_excluding()
def test_em_max_pairs_no_op_when_already_below(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = _make_linker(helper, fake_1000, "dedupe_only")
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    session = linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("surname"),
        max_pairs=1e9,  # far above the true pair count
        probe_proportion=0.5,
    )
    assert session._sample_threshold is None
    assert session._sample_info["sampling_applied"] is False
