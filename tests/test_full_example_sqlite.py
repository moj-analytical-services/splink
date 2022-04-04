from splink.sqlite.sqlite_linker import SQLiteLinker
import sqlite3
import os

import pandas as pd

from basic_settings import get_settings_dict


def test_full_example_sqlite(tmp_path):

    con = sqlite3.connect(":memory:")
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    df.to_sql("input_df_tablename", con)
    settings_dict = get_settings_dict()
    linker = SQLiteLinker(
        settings_dict,
        input_tables={"fake_data_1": "input_df_tablename"},
        connection=con,
    )

    linker.profile_columns(["first_name", "surname", "first_name || surname"])
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.train_u_using_random_sampling(target_rows=1e6)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    linker.splink_comparison_viewer(
        df_predict, os.path.join(tmp_path, "test_scv_sqlite.html"), True, 2
    )
