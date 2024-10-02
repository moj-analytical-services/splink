import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_score_missing_edges_dedupe(test_helpers, dialect):
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = SettingsCreator(
        link_type="dedupe_only",
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
    linker = Linker(df, settings, **helper.extra_linker_args())

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

@mark_with_dialects_excluding()
def test_score_missing_edges_link_only(test_helpers, dialect):
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = SettingsCreator(
        link_type="link_only",
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
    linker = Linker([df, df], settings, **helper.extra_linker_args())

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
