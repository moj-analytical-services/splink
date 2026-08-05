import pyarrow as pa
from pytest import fixture, mark

import splink.comparison_library as cl
from splink import SettingsCreator, block_on

from .decorator import mark_with_dialects_excluding


@fixture(scope="module")
def fake_200(fake_1000):
    # we don't need full data to check this logic - a smallish subset will do
    # as long as it's large enough to contain missed intra-cluster edges
    # when predicted with default parameters
    yield fake_1000[0:200]


@mark_with_dialects_excluding()
@mark.parametrize(
    ["link_type", "copies_of_df"],
    [["dedupe_only", 1], ["link_only", 2], ["link_and_dedupe", 2], ["link_only", 3]],
)
def test_score_missing_edges(test_helpers, dialect, link_type, copies_of_df, fake_200):
    helper = test_helpers[dialect]

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

    linker_input = [fake_200 for _ in range(copies_of_df)]
    linker = helper.linker_with_registration(linker_input, settings)

    df_predict = linker.inference.predict()
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.95
    )

    missing_edges = linker.inference._score_missing_cluster_edges(
        df_clusters,
        df_predict,
    ).as_record_list()

    assert len(missing_edges) > 0, "No missing edges found"
    assert not any(edge["surname_l"] == edge["surname_r"] for edge in missing_edges)
    assert not any(edge["dob_l"] == edge["dob_r"] for edge in missing_edges)


@mark_with_dialects_excluding()
@mark.parametrize(
    ["link_type", "copies_of_df"],
    [["dedupe_only", 1], ["link_only", 2]],
)
def test_score_missing_edges_all_edges(
    test_helpers, dialect, link_type, copies_of_df, fake_200
):
    helper = test_helpers[dialect]

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

    linker_input = [fake_200 for _ in range(copies_of_df)]
    linker = helper.linker_with_registration(linker_input, settings)

    df_predict = linker.inference.predict()
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.95
    )

    missing_edges = linker.inference._score_missing_cluster_edges(
        df_clusters,
    ).as_record_list()

    assert len(missing_edges) > 0, "No missing edges found"
    # some of these should be present now, as we are scoring all intracluster edges
    assert any(edge["surname_l"] == edge["surname_r"] for edge in missing_edges)
    assert any(edge["dob_l"] == edge["dob_r"] for edge in missing_edges)


@mark_with_dialects_excluding()
@mark.parametrize(
    ["link_type"],
    [["dedupe_only"], ["link_only"]],
)
def test_score_missing_edges_changed_column_names(
    test_helpers, dialect, link_type, fake_200
):
    helper = test_helpers[dialect]

    df = fake_200.rename_columns({"unique_id": "record_id"})
    df = df.append_column("sds", pa.array(200 * ["frame_1"]))
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
        unique_id_column_name="record_id",
        source_dataset_column_name="sds",
    )
    if link_type == "dedupe_only":
        linker_input = [df]
    else:
        df_2 = df.drop_columns("sds")
        df_2 = df_2.append_column("sds", pa.array(200 * ["frame_2"]))
        linker_input = [df, df_2]
    linker = helper.linker_with_registration(linker_input, settings)

    df_predict = linker.inference.predict()
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.95
    )

    missing_edges = linker.inference._score_missing_cluster_edges(
        df_clusters,
        df_predict,
    ).as_record_list()

    assert len(missing_edges) > 0, "No missing edges found"
    assert not any(edge["surname_l"] == edge["surname_r"] for edge in missing_edges)
    assert not any(edge["dob_l"] == edge["dob_r"] for edge in missing_edges)
