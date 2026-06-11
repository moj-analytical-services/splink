"""Tests for Linker initialisation and the basic end-to-end flow.

These mirror the logic of ``scripts/repro_duckdb_minimal.py`` but are written
using the backend-agnostic testing framework so they run against every
supported SQL backend, not just DuckDB.
"""

import splink.internals.comparison_library as cl
from splink import SettingsCreator, block_on

from .decorator import mark_with_dialects_excluding

DATA_CSV = "./tests/datasets/fake_1000_from_splink_demos.csv"

LINKER_COMPONENTS = (
    "clustering",
    "evaluation",
    "inference",
    "misc",
    "table_management",
    "training",
    "visualisations",
)


def _basic_settings():
    return SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.ExactMatch("email"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("surname"),
        ],
    )


def _make_linker(helper):
    df = helper.load_frame_from_csv(DATA_CSV)
    return helper.Linker(df, _basic_settings(), **helper.extra_linker_args())


@mark_with_dialects_excluding()
def test_linker_initialisation(test_helpers, dialect):
    helper = test_helpers[dialect]
    linker = _make_linker(helper)

    # All component namespaces are wired up on the linker.
    for component in LINKER_COMPONENTS:
        assert hasattr(linker, component), f"Linker missing component: {component}"

    # The dialect is resolved from the injected backend, not the settings dict.
    assert linker._settings_obj._sql_dialect_str == dialect


@mark_with_dialects_excluding()
def test_basic_end_to_end_flow(test_helpers, dialect):
    helper = test_helpers[dialect]
    linker = _make_linker(helper)

    # Training: lambda, u-values and m-values.
    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name", "surname")
    )

    # Inference.
    predictions = linker.inference.predict(threshold_match_probability=0.5)
    pred_pd = predictions.as_pandas_dataframe()
    assert len(pred_pd) > 0
    assert "match_probability" in pred_pd.columns
    # Probabilities are valid and respect the requested threshold.
    assert pred_pd["match_probability"].between(0.0, 1.0).all()
    assert pred_pd["match_probability"].min() >= 0.5

    # Clustering.
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions, threshold_match_probability=0.95
    )
    clusters_pd = clusters.as_pandas_dataframe()
    assert "cluster_id" in clusters_pd.columns
    # Every input record is assigned to exactly one cluster.
    assert len(clusters_pd) == 1000
    n_clusters = clusters_pd["cluster_id"].nunique()
    assert 0 < n_clusters <= len(clusters_pd)


@mark_with_dialects_excluding()
def test_probability_two_random_records_match_is_set(test_helpers, dialect):
    helper = test_helpers[dialect]
    linker = _make_linker(helper)

    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )

    prob = linker._settings_obj._probability_two_random_records_match
    assert 0.0 < prob < 1.0
