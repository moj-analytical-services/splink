import os
import sqlite3
from math import sqrt

import pandas as pd

from splink.exploratory import profile_columns
from splink.internals.linker import Linker
from splink.internals.sqlite.database_api import SQLiteAPI

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_including
from .linker_utils import _test_table_registration, register_roc_data


@mark_with_dialects_including("sqlite")
def test_full_example_sqlite(tmp_path):
    con = sqlite3.connect(":memory:")
    con.create_function("sqrt", 1, sqrt)

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings_dict = get_settings_dict()
    db_api = SQLiteAPI(con)
    df_sdf = db_api.register(df, source_dataset_name="fake_data_1")
    linker = Linker(
        df_sdf,
        settings_dict,
    )

    profile_columns(df_sdf, ["first_name", "surname", "first_name || surname"])

    linker.table_management.compute_tf_table("city")
    linker.table_management.compute_tf_table("first_name")

    linker.training.estimate_probability_two_random_records_match(
        ["l.email = r.email"], recall=0.3
    )

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6, seed=1)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.inference.predict()

    linker.visualisations.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "test_scv_sqlite.html"), True, 2
    )

    linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.5)

    linker.evaluation.unlinkables_chart(name_of_data_in_title="Testing")

    _test_table_registration(linker)

    register_roc_data(linker)

    linker.evaluation.accuracy_analysis_from_labels_table("labels")


@mark_with_dialects_including("sqlite")
def test_small_link_example_sqlite():
    con = sqlite3.connect(":memory:")
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings_dict = get_settings_dict()

    settings_dict["link_type"] = "link_only"

    db_api = SQLiteAPI(con)
    df_1_sdf = db_api.register(df, source_dataset_name="fake_data_1")
    df_2_sdf = db_api.register(df, source_dataset_name="fake_data_2")
    linker = Linker(
        [df_1_sdf, df_2_sdf],
        settings_dict,
    )

    linker.inference.predict()


@mark_with_dialects_including("sqlite")
def test_default_conn_sqlite(tmp_path):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings_dict = get_settings_dict()

    db_api = SQLiteAPI()
    df_sdf = db_api.register(df)
    linker = Linker(df_sdf, settings_dict)

    linker.inference.predict()
