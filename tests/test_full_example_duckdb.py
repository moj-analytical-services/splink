import os

import pandas as pd
import pyarrow.parquet as pq
from basic_settings import get_settings_dict
from linker_utils import _test_table_registration, register_roc_data

import splink.duckdb.duckdb_comparison_level_library as cll
from splink.duckdb.duckdb_comparison_library import (
    exact_match,
    jaccard_at_thresholds,
    jaro_winkler_at_thresholds,
)
from splink.duckdb.duckdb_linker import DuckDBLinker


# @pytest.mark.parametrize(['first_name_and_surname_cc'], [(cll)])
def test_full_example_duckdb(tmp_path, first_name_and_surname_cc):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = df.rename(columns={"surname": "SUR name"})
    settings_dict = get_settings_dict()

    # Overwrite the surname comparison to include duck-db specific syntax
    settings_dict["comparisons"][0] = jaro_winkler_at_thresholds("first_name")
    settings_dict["comparisons"][1] = jaccard_at_thresholds("SUR name")
    settings_dict["comparisons"].append(first_name_and_surname_cc(cll, "SUR name"))
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
    linker.estimate_probability_two_random_records_match(
        ["l.email = r.email"], recall=0.3
    )
    # try missingness chart again now that concat_with_tf is precomputed
    linker.missingness_chart()

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

    register_roc_data(linker)
    linker.roc_chart_from_labels_table("labels")

    df_clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.1)

    linker.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        sampling_method="by_cluster_size",
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.unlinkables_chart(source_dataset="Testing")

    _test_table_registration(linker)

    record = {
        "unique_id": 1,
        "first_name": "John",
        "SUR name": "Smith",
        "dob": "1971-05-24",
        "city": "London",
        "email": "john@smith.net",
        "group": 10000,
    }

    linker.find_matches_to_new_records(
        [record], blocking_rules=[], match_weight_threshold=-10000
    )

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.save_settings_to_json(path)

    linker_2 = DuckDBLinker(df, connection=":memory:")
    linker_2.load_settings_from_json(path)
    DuckDBLinker(df, settings_dict=path)


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
