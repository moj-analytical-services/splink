import pyarrow as pa
import pyarrow.csv as pv
from pytest import mark, raises

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding

df = pv.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
# we just want to check it runs, so use a small slice of the data
df = df.slice(length=25)
df_l = df.append_column("source_dataset", pa.array(25 * ["my_left_ds"]))
df_r = df.append_column("source_dataset", pa.array(25 * ["my_right_ds"]))
df_m = df.append_column("source_dataset", pa.array(25 * ["my_moddle_ds"]))
df_combined = pa.concat_tables([df_l, df_r])


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
    linker = helper.linker_with_registration(input_pd_tables, settings)

    df_predict = linker.inference.predict()
    linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.95)


def test_clustering_mw_prob_equivalence(fake_1000):
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)
    settings_dict = get_settings_dict()
    linker = Linker(df_sdf, settings_dict)

    df_predict = linker.inference.predict()

    clusters_mw = (
        linker.clustering.cluster_pairwise_predictions_at_threshold(
            df_predict, threshold_match_weight=4.2479
        )
        .query_sql("SELECT * FROM {this} ORDER BY unique_id")
        .as_dict()
    )

    clusters_prob = (
        linker.clustering.cluster_pairwise_predictions_at_threshold(
            df_predict, threshold_match_probability=0.95
        )
        .query_sql("SELECT * FROM {this} ORDER BY unique_id")
        .as_dict()
    )

    assert clusters_mw["cluster_id"] == clusters_prob["cluster_id"]
    assert clusters_mw["unique_id"] == clusters_prob["unique_id"]

    with raises(ValueError, match="Please specify only one"):
        linker.clustering.cluster_pairwise_predictions_at_threshold(
            df_predict, threshold_match_weight=3, threshold_match_probability=0.95
        )


@mark_with_dialects_excluding()
def test_clustering_no_edges(test_helpers, dialect):
    helper = test_helpers[dialect]

    data = [
        {"id": 1, "first_name": "Andy", "surname": "Bandy", "city": "London"},
        {"id": 2, "first_name": "Andi", "surname": "Bandi", "city": "London"},
        {"id": 3, "first_name": "Terry", "surname": "Berry", "city": "Glasgow"},
        {"id": 4, "first_name": "Terri", "surname": "Berri", "city": "Glasgow"},
    ]

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
    linker = helper.linker_with_registration([data], settings)

    # due to blocking rules, df_predict will be empty
    df_predict = linker.inference.predict()
    linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.95)
