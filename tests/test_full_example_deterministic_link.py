import os

import pandas as pd

from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)
from splink.internals.linker import Linker

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_deterministic_link_full_example(dialect, tmp_path, test_helpers):
    helper = test_helpers[dialect]
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = helper.convert_frame(df)

    br_for_predict = [
        "l.first_name = r.first_name and l.surname = r.surname and l.dob = r.dob",
        "l.surname = r.surname and l.dob = r.dob and l.email = r.email",
        "l.first_name = r.first_name and l.surname = r.surname "
        "and l.email = r.email",
    ]
    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": br_for_predict,
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
        table_or_tables=df,
        blocking_rules=br_for_predict,
        link_type="dedupe_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
    )

    linker = Linker(df, settings, **helper.extra_linker_args())

    df_predict = linker.inference.deterministic_link()

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict)

    linker.visualisations.cluster_studio_dashboard(
        df_predict,
        clusters,
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
        sampling_method="by_cluster_size",
        overwrite=True,
    )
