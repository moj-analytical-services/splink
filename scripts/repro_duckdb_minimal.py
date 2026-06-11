"""Minimal DuckDB reproduction script for Splink.

Verifies that the Linker initialises correctly against the DuckDB backend and
that the core end-to-end flow works: training -> inference -> clustering.

Run from the repo root with:

    uv run python scripts/repro_duckdb_minimal.py

It exits non-zero (via assertions) if any stage misbehaves.
"""

from __future__ import annotations

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets


def main() -> None:
    # 1. Backend + data ----------------------------------------------------
    db_api = DuckDBAPI()  # in-memory DuckDB connection
    df = splink_datasets.fake_1000
    print(f"Loaded fake_1000: {len(df)} rows, columns={list(df.columns)}")  # noqa: T201

    # 2. Settings ----------------------------------------------------------
    settings = SettingsCreator(
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

    # 3. Linker initialisation --------------------------------------------
    linker = Linker(df, settings, db_api)

    # Verify the Linker wired up its component objects and backend correctly.
    for component in (
        "clustering",
        "evaluation",
        "inference",
        "misc",
        "table_management",
        "training",
        "visualisations",
    ):
        assert hasattr(linker, component), f"Linker missing component: {component}"
    assert linker._db_api is db_api, "Linker did not retain the injected db_api"
    assert (
        linker._settings_obj._sql_dialect_str == "duckdb"
    ), "Settings dialect was not resolved from the DuckDB backend"
    print("Linker initialised: all 7 components present, dialect=duckdb")  # noqa: T201

    # 4. Training ----------------------------------------------------------
    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name", "surname")
    )
    print("Training completed: lambda, u-values and m-values estimated")  # noqa: T201

    # 5. Inference ---------------------------------------------------------
    predictions = linker.inference.predict(threshold_match_probability=0.5)
    pred_pd = predictions.as_pandas_dataframe()
    assert len(pred_pd) > 0, "predict() returned zero pairwise comparisons"
    assert "match_probability" in pred_pd.columns
    print(f"Inference completed: {len(pred_pd)} pairwise predictions")  # noqa: T201

    # 6. Clustering --------------------------------------------------------
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions, threshold_match_probability=0.95
    )
    clusters_pd = clusters.as_pandas_dataframe()
    assert "cluster_id" in clusters_pd.columns
    n_clusters = clusters_pd["cluster_id"].nunique()
    print(f"Clustering completed: {len(clusters_pd)} rows -> {n_clusters} clusters")  # noqa: T201, E501

    print("\nSUCCESS: Linker init and basic DuckDB functionality verified.")  # noqa: T201


if __name__ == "__main__":
    main()
