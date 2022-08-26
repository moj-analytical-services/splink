from splink.sqlite.sqlite_linker import SQLiteLinker
import os
import sqlite3
import pandas as pd

from basic_settings import get_settings_dict

from splink.comparison_level_library import (
    _mutable_params,
)

_mutable_params["dialect"] = "sqlite"


def test_full_example_sqlite(tmp_path):

    from rapidfuzz.distance.Levenshtein import distance

    con = sqlite3.connect(":memory:")
    con.create_function("levenshtein", 2, distance)
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

    linker.estimate_u_using_random_sampling(target_rows=1e6)

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


def test_small_link_example_sqlite():

    from rapidfuzz.distance.Levenshtein import distance

    con = sqlite3.connect(":memory:")
    con.create_function("levenshtein", 2, distance)

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
