import pandas as pd

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_single_best_links_correctness(test_helpers, dialect):
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        {
            "unique_id": [0, 1, 2, 3, 4, 5, 6, 7, 8],
            "source_dataset": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
        }
    )

    predictions = pd.DataFrame(
        {
            "unique_id_l": [0, 1, 3, 4, 6, 6],
            "unique_id_r": [1, 2, 5, 5, 5, 7],
            "source_dataset_l": ["a", "b", "a", "b", "a", "a"],
            "source_dataset_r": ["b", "c", "c", "c", "c", "b"],
            "match_probability": [0.90, 0.70, 0.85, 0.90, 0.80, 0.70],
        }
    )

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[],
        blocking_rules_to_generate_predictions=[],
    )

    linker = Linker(df, settings, **helper.extra_linker_args())

    df_predict = linker.table_management.register_table_predict(
        predictions, overwrite=True
    )

    df_clusters = linker.clustering.cluster_using_single_best_links(
        df_predict, source_datasets=["a", "b", "c"], threshold_match_probability=0.5
    )

    result = df_clusters.as_pandas_dataframe().sort_values("unique_id")
    result = result.reset_index(drop=True)

    correct_result = pd.DataFrame(
        {
            "cluster_id": [
                "a-__-0",
                "a-__-0",
                "a-__-0",
                "a-__-3",
                "a-__-3",
                "a-__-3",
                "a-__-6",
                "b-__-7",
                "c-__-8",
            ],
            "unique_id": [0, 1, 2, 3, 4, 5, 6, 7, 8],
            "source_dataset": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
        }
    )
    correct_result = correct_result.sort_values("unique_id")
    correct_result = correct_result.reset_index(drop=True)

    pd.testing.assert_frame_equal(result, correct_result)


@mark_with_dialects_excluding()
def test_single_best_links_ties(test_helpers, dialect):
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        {
            "unique_id": [0, 1, 2],
            "source_dataset": ["a", "a", "b"],
        }
    )

    predictions = pd.DataFrame(
        {
            "unique_id_l": [0, 1],
            "unique_id_r": [2, 2],
            "source_dataset_l": ["a", "a"],
            "source_dataset_r": ["b", "b"],
            "match_probability": [0.90, 0.90],
        }
    )

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[],
        blocking_rules_to_generate_predictions=[],
    )

    linker = Linker(df, settings, **helper.extra_linker_args())

    df_predict = linker.table_management.register_table_predict(
        predictions, overwrite=True
    )

    df_clusters = linker.clustering.cluster_using_single_best_links(
        df_predict, source_datasets=["a", "b"], threshold_match_probability=0.5
    )

    result = df_clusters.as_pandas_dataframe()
    n_clusters = result["cluster_id"].nunique()

    assert n_clusters > 1


@mark_with_dialects_excluding()
def test_single_best_links_one_to_one(test_helpers, dialect):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df_l = df.copy()
    df_r = df.copy()
    df_l["source_dataset"] = "a"
    df_r["source_dataset"] = "b"

    helper = test_helpers[dialect]

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
    )

    linker = Linker([df_l, df_r], settings, **helper.extra_linker_args())

    linker.training.estimate_u_using_random_sampling(1e6)

    df_predict = linker.inference.predict(0.5)

    df_clusters = linker.clustering.cluster_using_single_best_links(
        df_predict, source_datasets=["a", "b"], threshold_match_probability=0.5
    )

    result = linker.misc.query_sql(
        f"""
        with t as (
            select
                cluster_id,
                sum(cast(source_dataset = 'a' as int)) as count_a,
                sum(cast(source_dataset = 'b' as int)) as count_b
            from {df_clusters.physical_name}
            group by cluster_id
        )
        select count(*) as count
        from t
        where count_a > 1 or count_b > 1
        """
    )

    count = result["count"][0]
    assert count == 0
