import os

from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd

from basic_settings import get_settings_dict


def test_full_example_duckdb(tmp_path):

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings_dict = get_settings_dict()

    # Overwrite the surname comparison to include duck-db specific syntax
    surname_match_level = {
        "sql_condition": "jaccard(surname_l, surname_r)",
        # "sql_condition": "surname_l similar to surname_r",
        "label_for_charts": "Exact match",
        "m_probability": 0.9,
        "u_probability": 0.1,
    }

    settings_dict["comparisons"][1]["comparison_levels"][1] = surname_match_level

    linker = DuckDBLinker(
        df,
        settings_dict,
        connection=os.path.join(tmp_path, "duckdb.db"),
        output_schema="splink_in_duckdb",
    )

    # linker.analyse_blocking_rule(
    #     "l.first_name = r.first_name and l.surname = r.surname"
    # )

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
    )
    linker.missingness_chart()
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.train_u_using_random_sampling(target_rows=1e6)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    linker.splink_comparison_viewer(
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

    linker.con.register("labels", df_labels)
    # Finish create labels

    linker.roc_from_labels("labels")
