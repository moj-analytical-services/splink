import os

from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.duckdb.duckdb_comparison_library import jaccard_at_thresholds, exact_match
from splink.duckdb.duckdb_comparison_level_library import _mutable_params
import pandas as pd
import pyarrow.parquet as pq
from basic_settings import get_settings_dict

from splink.comparison_level_library import (
    _mutable_params,
)

_mutable_params["dialect"] = "duckdb"


def test_full_example_duckdb(tmp_path):

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = df.rename(columns={"surname": "SUR name"})
    settings_dict = get_settings_dict()

    # Overwrite the surname comparison to include duck-db specific syntax

    _mutable_params["dialect"] = "duckdb"
    settings_dict["comparisons"][1] = jaccard_at_thresholds("SUR name")
    settings_dict["blocking_rules_to_generate_predictions"] = [
        'l."SUR name" = r."SUR name"',
    ]

    linker = DuckDBLinker(
        df,
        settings_dict,
        connection=os.path.join(tmp_path, "duckdb.db"),
        output_schema="splink_in_duckdb",
    )

    linker.count_num_comparisons_from_blocking_rule(
        'l.first_name = r.first_name and l."SUR name" = r."SUR name"'
    )

    linker.profile_columns(
        [
            "first_name",
            '"SUR name"',
            'first_name || "SUR name"',
            "concat(city, first_name)",
        ]
    )
    linker.missingness_chart()
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_u_using_random_sampling(target_rows=1e6)

    blocking_rule = 'l.first_name = r.first_name and l."SUR name" = r."SUR name"'
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    linker.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "test_scv_duckdb.html"), True, 2
    )

    df_e = df_predict.as_pandas_dataframe(limit=5)
    records = df_e.to_dict(orient="records")
    linker.waterfall_chart(records)

    # Create labels
    df_10 = df.head(10).copy()
    df_10["merge"] = 1
    df_10["source_dataset"] = "fake_data_1"

    df_l = df_10[["unique_id", "source_dataset", "group", "merge"]].copy()
    df_r = df_l.copy()

    df_labels = df_l.merge(df_r, on="merge", suffixes=("_l", "_r"))
    f1 = df_labels["unique_id_l"] < df_labels["unique_id_r"]
    df_labels = df_labels[f1]

    df_labels["clerical_match_score"] = (
        df_labels["group_l"] == df_labels["group_r"]
    ).astype(float)

    df_labels = df_labels.drop(
        ["group_l", "group_r", "source_dataset_l", "source_dataset_r", "merge"],
        axis=1,
    )

    linker._con.register("labels", df_labels)
    # Finish create labels

    linker.roc_chart_from_labels("labels")

    df_clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.1)

    linker.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        sampling_method="by_cluster_size",
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.unlinkables_chart(source_dataset="Testing")

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.save_settings_to_json(path)

    linker_2 = DuckDBLinker(df, connection=":memory:")
    linker_2.load_settings_from_json(path)


def test_small_link_example_duckdb():

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings_dict = get_settings_dict()

    settings_dict["link_type"] = "link_only"

    linker = DuckDBLinker(
        [df, df],
        settings_dict,
        connection=":memory:",
        output_schema="splink_in_duckdb",
    )

    linker.predict()


def test_duckdb_load_from_file():

    settings = get_settings_dict()

    f = "./tests/datasets/fake_1000_from_splink_demos.csv"

    linker = DuckDBLinker(
        f,
        settings,
    )

    assert len(linker.predict().as_pandas_dataframe()) == 3167

    settings["link_type"] = "link_only"

    linker = DuckDBLinker(
        [f, f],
        settings,
        input_table_aliases=["testing1", "testing2"],
    )

    assert len(linker.predict().as_pandas_dataframe()) == 7257


def test_duckdb_arrow_array():

    # Checking array fixes problem identified here:
    # https://github.com/moj-analytical-services/splink/issues/680

    f = "./tests/datasets/test_array.parquet"
    array_data = pq.read_table(f)

    # data is:
    # data_list = [
    # {"uid": 1, "a": ['robin', 'john'], "b": 1},
    # {"uid": 1, "a": ['robin', 'john'], "b": 1},
    # {"uid": 1, "a": ['james', 'karen'], "b": 1},
    # {"uid": 1, "a": ['james', 'john'], "b": 1},
    #     ]

    linker = DuckDBLinker(
        array_data,
        {
            "link_type": "dedupe_only",
            "unique_id_column_name": "uid",
            "comparisons": [exact_match("b")],
            "blocking_rules_to_generate_predictions": ["l.a[1] = r.a[1]"],
        },
    )
    df = linker.deterministic_link().as_pandas_dataframe()
    assert len(df) == 2
