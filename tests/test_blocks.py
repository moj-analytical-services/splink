import pytest
from sparklink.blocking import sql_gen_block_using_rules
import sqlite3
import pandas as pd
from pandas.util.testing import assert_frame_equal

@pytest.fixture(scope='module')
def db():
    # Create the database and the database table
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("create table test (unique_id, first_name, surname)")
    cur.execute("insert into test values (?, ?, ?)", (1, "robin", "linacre"))
    cur.execute("insert into test values (?, ?, ?)", (2, "john", "smith"))
    cur.execute("insert into test values (?, ?, ?)", (3, "john", "linacre"))
    cur.execute("insert into test values (?, ?, ?)", (4, "john", "smith"))
    cur.execute("insert into test values (?, ?, ?)", (5, None, "smith"))
    cur.execute("insert into test values (?, ?, ?)", (6, "john", None))

    yield cur


def test_blocking_rules_sql_gen(db):

    sql = "select * from test limit 1"
    db.execute(sql)
    one = db.fetchone()
    columns = one.keys()

    rules = [
        "l.surname = r.surname",
        "l.first_name = r.first_name"
    ]

    sql = sql_gen_block_using_rules("dedupe_only", columns, rules, table_name_dedupe="test")

    db.execute(sql)
    result = db.fetchall()
    result = [dict(r) for r in result]

    correct_answer = [
        {'unique_id_l': 1, 'unique_id_r': 3},
        {'unique_id_l': 2, 'unique_id_r': 3},
        {'unique_id_l': 2, 'unique_id_r': 4},
        {'unique_id_l': 3, 'unique_id_r': 4},
        {'unique_id_l': 2, 'unique_id_r': 5},
        {'unique_id_l': 4, 'unique_id_r': 5},
        {'unique_id_l': 2, 'unique_id_r': 6},
        {'unique_id_l': 3, 'unique_id_r': 6},
        {'unique_id_l': 4, 'unique_id_r': 6}
        ]

    pd_correct = pd.DataFrame(correct_answer)[["unique_id_l", "unique_id_r"]]
    pd_correct = pd_correct.sort_values(["unique_id_l", "unique_id_r"])
    pd_result = pd.DataFrame(result)[["unique_id_l", "unique_id_r"]]
    pd_result = pd_result.sort_values(["unique_id_l", "unique_id_r"])

    assert_frame_equal(pd_correct.reset_index(drop=True), pd_result.reset_index(drop=True))

