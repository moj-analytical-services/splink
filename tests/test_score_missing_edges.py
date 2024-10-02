from pytest import mark

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
@mark.parametrize(
    ["link_type", "copies_of_df"],
    [["dedupe_only", 1], ["link_only", 2], ["link_and_dedupe", 2], ["link_only", 3]],
)
def test_score_missing_edges_dedupe(test_helpers, dialect, link_type, copies_of_df):
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = SettingsCreator(
        link_type=link_type,
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("surname"),
            block_on("dob"),
        ],
        retain_intermediate_calculation_columns=True,
    )
    linker_input = df if copies_of_df == 1 else [df for _ in range(copies_of_df)]
    linker = Linker(linker_input, settings, **helper.extra_linker_args())

    df_predict = linker.inference.predict()
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.95
    )

    df_missing_edges = linker.inference.score_missing_cluster_edges(
        df_clusters,
        df_predict,
    ).as_pandas_dataframe()

    assert not df_missing_edges.empty, "No missing edges found"
    assert not any(df_missing_edges["surname_l"] == df_missing_edges["surname_r"])
    assert not any(df_missing_edges["dob_l"] == df_missing_edges["dob_r"])
