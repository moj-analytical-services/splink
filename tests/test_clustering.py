import pandas as pd
import pytest
from pytest import mark

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink.clustering import (
    cluster_pairwise_predictions_at_multiple_thresholds
)

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
# we just want to check it runs, so use a small slice of the data
df = df[0:25]
df_l = df.copy()
df_r = df.copy()
df_m = df.copy()
df_l["source_dataset"] = "my_left_ds"
df_r["source_dataset"] = "my_right_ds"
df_m["source_dataset"] = "my_middle_ds"
df_combined = pd.concat([df_l, df_r])


@mark_with_dialects_excluding()
@mark.parametrize(
    ["link_type", "input_pd_tables"],
    [
        ["dedupe_only", [df]],
        ["link_only", [df, df]],  # no source dataset
        ["link_only", [df_l, df_r]],  # source dataset column
        ["link_only", [df_combined]],  # concatenated frame
        ["link_only", [df_l, df_m, df_r]],
        ["link_and_dedupe", [df, df]],  # no source dataset
        ["link_and_dedupe", [df_l, df_r]],  # source dataset column
        ["link_and_dedupe", [df_combined]],  # concatenated frame
    ],
    ids=[
        "dedupe",
        "link_only_no_source_dataset",
        "link_only_with_source_dataset",
        "link_only_concat",
        "link_only_three_tables",
        "link_and_dedupe_no_source_dataset",
        "link_and_dedupe_with_source_dataset",
        "link_and_dedupe_concat",
    ],
)
def test_clustering(test_helpers, dialect, link_type, input_pd_tables):
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
    )
    linker_input = list(map(helper.convert_frame, input_pd_tables))
    linker = Linker(linker_input, settings, **helper.extra_linker_args())

    df_predict = linker.inference.predict()
    linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.95)


def test_clustering_mw_prob_equivalence():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    db_api = DuckDBAPI()
    settings_dict = get_settings_dict()
    linker = Linker(df, settings_dict, db_api=db_api)

    df_predict = linker.inference.predict()

    clusters_mw = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, threshold_match_weight=4.2479
    ).as_pandas_dataframe()

    clusters_prob = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, threshold_match_probability=0.95
    ).as_pandas_dataframe()

    pd.testing.assert_series_equal(
        clusters_mw["cluster_id"], clusters_prob["cluster_id"]
    )
    pd.testing.assert_series_equal(clusters_mw["unique_id"], clusters_prob["unique_id"])

    with pytest.raises(ValueError, match="Please specify only one"):
        linker.clustering.cluster_pairwise_predictions_at_threshold(
            df_predict, threshold_match_weight=3, threshold_match_probability=0.95
        )


@mark_with_dialects_excluding()
def test_clustering_no_edges(test_helpers, dialect):
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        [
            {"id": 1, "first_name": "Andy", "surname": "Bandy", "city": "London"},
            {"id": 2, "first_name": "Andi", "surname": "Bandi", "city": "London"},
            {"id": 3, "first_name": "Terry", "surname": "Berry", "city": "Glasgow"},
            {"id": 4, "first_name": "Terri", "surname": "Berri", "city": "Glasgow"},
        ]
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("city"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("surname"),
            block_on("first_name"),
        ],
        unique_id_column_name="id",
    )
    linker_input = helper.convert_frame(df)
    linker = Linker(linker_input, settings, **helper.extra_linker_args())

    # due to blocking rules, df_predict will be empty
    df_predict = linker.inference.predict()
    linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.95)

@mark_with_dialects_excluding()
@mark.parametrize(
    ["link_type", "input_pd_tables"],
    [
        ["dedupe_only", [df]],
        ["link_only", [df, df]],  # no source dataset
        ["link_only", [df_l, df_r]],  # source dataset column
        ["link_only", [df_combined]],  # concatenated frame
        ["link_only", [df_l, df_m, df_r]],
        ["link_and_dedupe", [df, df]],  # no source dataset
        ["link_and_dedupe", [df_l, df_r]],  # source dataset column
        ["link_and_dedupe", [df_combined]],  # concatenated frame
    ],
    ids=[
        "dedupe",
        "link_only_no_source_dataset",
        "link_only_with_source_dataset",
        "link_only_concat",
        "link_only_three_tables",
        "link_and_dedupe_no_source_dataset",
        "link_and_dedupe_with_source_dataset",
        "link_and_dedupe_concat",
    ],
)
def test_clustering_multi_thresholds(test_helpers, dialect, link_type, input_pd_tables):
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
    )
    linker_input = list(map(helper.convert_frame, input_pd_tables))
    linker = Linker(linker_input, settings, **helper.extra_linker_args())

    df_predict = linker.inference.predict()
    linker.clustering.cluster_pairwise_predictions_at_multiple_thresholds(df_predict, [0.5, 0.95])


def test_clustering_single_multi_threshold_equivalence():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    db_api = DuckDBAPI()
    settings_dict = get_settings_dict()
    linker = Linker(df, settings_dict, db_api=db_api)

    df_predict = linker.inference.predict()

    clusters_0_5 = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.5).as_pandas_dataframe()
    clusters_0_9 = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.9).as_pandas_dataframe()

    clusters_multi = linker.clustering.cluster_pairwise_predictions_at_multiple_thresholds(df_predict, [0.5, 0.9]).as_pandas_dataframe()

    df = pd.merge(clusters_0_5, clusters_multi, left_on='unique_id', right_on='unique_id', how='inner')

    df["different"] = df["cluster_id"] != df["cluster_p_0_9"]
    compare = df[["cluster_id", "cluster_p_0_9", "different"]]
    df.sort_values(by='different', ascending=False, inplace=True)
    print(compare[compare["different"]==True])
    print(sum(compare["different"]))

    assert clusters_0_5["cluster_id"].equals(clusters_multi["cluster_p_0_5"])
    assert clusters_0_9["cluster_id"].equals(clusters_multi["cluster_p_0_9"])



def test_clustering_multi_threshold_linker_non_linker_equivalence():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    db_api = DuckDBAPI()
    settings_dict = get_settings_dict()
    linker = Linker(df, settings_dict, db_api=db_api)

    df_predict = linker.inference.predict()

    clusters_linker = linker.clustering.cluster_pairwise_predictions_at_multiple_thresholds(
        df_predict, 
        [0.5, 0.95]
        ).as_pandas_dataframe()
    clusters_non_linker = cluster_pairwise_predictions_at_multiple_thresholds(
                            df,
                            df_predict,
                            node_id_column_name="unique_id",
                            edge_id_column_name_left="unique_id_l",
                            edge_id_column_name_right="unique_id_r",
                            db_api=linker._db_api,
                            match_probability_thresholds=[0.5, 0.95]
                        ).as_pandas_dataframe()
    df = pd.DataFrame({'linker': clusters_linker['cluster_p_0_5'], 'non-linker': clusters_non_linker['cluster_p_0_5']})
    # df["different"] = df["linker"] != df["non-linker"]
    # df.sort_values(by='different', ascending=False, inplace=True)
    # print(df)
    # print(sum(df["different"]))

    #assert clusters_linker["cluster_p_0_5"].equals(clusters_non_linker["cluster_p_0_5"])
    #assert clusters_linker["cluster_p_0_95"].equals(clusters_non_linker["cluster_p_0_95"])