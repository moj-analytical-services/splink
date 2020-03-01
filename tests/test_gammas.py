import sqlite3

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from splink.gammas import sql_gen_add_gammas, complete_settings_dict


@pytest.fixture(scope="module")
def db():
    # Create the database and the database table
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("create table test1 (fname_l, fname_r)")
    cur.execute("insert into test1 values (?, ?)", ("robin", "robin"))
    cur.execute("insert into test1 values (?, ?)", ("robin", "john"))

    cur.execute(
        "create table test2 (unique_id_l, unique_id_r, fname_l, fname_r, sname_l, sname_r)"
    )
    cur.execute(
        "insert into test2 values (?, ?, ?, ?, ?, ?)",
        (1, 2, "robin", "robin", "linacre", "linacre"),
    )
    cur.execute(
        "insert into test2 values (?, ?, ?, ?, ?, ?)",
        (3, 4, "robin", "robin", "linacrr", "linacre"),
    )
    cur.execute(
        "insert into test2 values (?, ?, ?, ?, ?, ?)",
        (5, 6, None, None, None, "linacre"),
    )
    cur.execute(
        "insert into test2 values (?, ?, ?, ?, ?, ?)",
        (7, 8, "robin", "julian", "linacre", "smith"),
    )

    yield cur


def test_add_gammas(db):

    gamma_settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.5,
        "comparison_columns": [
            {"col_name": "fname", "num_levels": 2},
            {
                "col_name": "sname",
                "num_levels": 3,
                "case_expression": """
                                    case
                                    when sname_l = sname_r then 2
                                    when substr(sname_l,1, 3) =  substr(sname_r, 1, 3) then 1
                                    else 0
                                    end
                                    as gamma_sname
                                    """
            },
        ],
        "blocking_rules": [],
        "retain_matching_columns": False
    }

    gamma_settings = complete_settings_dict(gamma_settings, spark="supress_warnings")

    sql = sql_gen_add_gammas(
        gamma_settings, table_name="test2"
    )
    db.execute(sql)
    result = db.fetchall()
    result = [dict(r) for r in result]

    correct_answer = [
        {"unique_id_l": 1, "unique_id_r": 2, "gamma_fname": 1, "gamma_sname": 2},
        {"unique_id_l": 3, "unique_id_r": 4, "gamma_fname": 1, "gamma_sname": 1},
        {"unique_id_l": 5, "unique_id_r": 6, "gamma_fname": -1, "gamma_sname": -1},
        {"unique_id_l": 7, "unique_id_r": 8, "gamma_fname": 0, "gamma_sname": 0},
    ]

    pd_correct = pd.DataFrame(correct_answer)
    pd_correct = pd_correct.sort_values(["unique_id_l", "unique_id_r"])
    pd_result = pd.DataFrame(result)
    pd_result = pd_result.sort_values(["unique_id_l", "unique_id_r"])

    assert_frame_equal(pd_correct, pd_result)

    gamma_settings["retain_matching_columns"] = True
    sql = sql_gen_add_gammas(gamma_settings, table_name="test2")

    db.execute(sql)
    result = db.fetchone()
    col_names = list(dict(result).keys())
    correct_col_names = [
        "unique_id_l",
        "unique_id_r",
        "fname_l",
        "fname_r",
        "gamma_fname",
        "sname_l",
        "sname_r",
        "gamma_sname",
    ]
    assert col_names == correct_col_names
