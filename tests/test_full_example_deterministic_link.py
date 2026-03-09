import os

import pandas as pd

import splink.internals.comparison_library as cl
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

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
    helper_db_api = helper.db_api()
    df_sdf = helper_db_api.register(df)

    cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
        df_sdf,
        blocking_rules=br_for_predict,
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
    )

    linker = helper.linker_with_registration(df, settings)

    df_predict = linker.inference.deterministic_link()

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict)

    linker.visualisations.cluster_studio_dashboard(
        df_predict,
        clusters,
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
        sampling_method="by_cluster_size",
        overwrite=True,
    )


@mark_with_dialects_excluding()
def test_deterministic_link_uses_lowest_numeric_match_key(test_helpers, dialect):
    helper = test_helpers[dialect]

    data = {
        "unique_id": [1, 2],
        **{
            f"c{i}": [1, 1] if i in (2, 10) else [100 + i, 200 + i]
            for i in range(11)
        },
    }

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.ExactMatch("c2")],
        "blocking_rules_to_generate_predictions": [
            f"l.c{i} = r.c{i}" for i in range(11)
        ],
    }

    linker = helper.linker_with_registration(data, settings)
    records = linker.inference.deterministic_link().as_record_dict()

    assert len(records) == 1
    assert int(records[0]["match_key"]) == 2
