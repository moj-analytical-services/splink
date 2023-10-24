import os
import sqlite3
from math import sqrt

import pandas as pd

from splink.sqlite.linker import SQLiteLinker

from .basic_settings import get_settings_dict
from .linker_utils import _test_table_registration, register_roc_data


def test_full_example_sqlite(tmp_path):
    con = sqlite3.connect(":memory:")
    con.create_function("sqrt", 1, sqrt)

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    df.to_sql("input_df_tablename", con)

    settings_dict = get_settings_dict()
    linker = SQLiteLinker(
        "input_df_tablename",
        settings_dict,
        connection=con,
        input_table_aliases="fake_data_1",
    )

    linker.profile_columns(["first_name", "surname", "first_name || surname"])
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_probability_two_random_records_match(
        ["l.email = r.email"], recall=0.3
    )

    linker.estimate_u_using_random_sampling(max_pairs=1e6, seed=1)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    linker.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "test_scv_sqlite.html"), True, 2
    )

    linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.5)

    linker.unlinkables_chart(source_dataset="Testing")

    _test_table_registration(linker)

    register_roc_data(linker)
    linker.roc_chart_from_labels_table("labels")
    linker.accuracy_chart_from_labels_table("labels")
    linker.confusion_matrix_from_labels_table("labels")


def test_small_link_example_sqlite():
    con = sqlite3.connect(":memory:")
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings_dict = get_settings_dict()

    settings_dict["link_type"] = "link_only"

    df.to_sql("input_df_tablename", con)

    linker = SQLiteLinker(
        ["input_df_tablename", "input_df_tablename"],
        settings_dict,
        connection=con,
        input_table_aliases=["fake_data_1", "fake_data_2"],
    )

    linker.predict()


def test_default_conn_sqlite(tmp_path):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings_dict = get_settings_dict()
    linker = SQLiteLinker(df, settings_dict)

    linker.predict()
