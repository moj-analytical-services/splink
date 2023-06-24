import os

import pandas as pd
import pytest

from splink.duckdb.linker import DuckDBLinker
from splink.spark.linker import SparkLinker


@pytest.mark.parametrize(
    ("Linker"),
    [
        pytest.param(DuckDBLinker, id="DuckDB Deterministic Link Test"),
        pytest.param(SparkLinker, id="Spark Deterministic Link Test"),
    ],
)
def test_deterministic_link_full_example(tmp_path, spark, Linker):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name and l.surname = r.surname and l.dob = r.dob",
            "l.surname = r.surname and l.dob = r.dob and l.email = r.email",
            "l.first_name = r.first_name and l.surname = r.surname "
            "and l.email = r.email",
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    if Linker == SparkLinker:
        # ensure the same datatype within a column to solve spark parsing issues
        df = df.astype(str)

        df = spark.createDataFrame(df)
        df.persist()
    linker = Linker(df, settings)

    linker.cumulative_num_comparisons_from_blocking_rules_chart()

    df_predict = linker.deterministic_link()

    clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict)

    linker.cluster_studio_dashboard(
        df_predict,
        clusters,
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
        sampling_method="by_cluster_size",
        overwrite=True,
    )
