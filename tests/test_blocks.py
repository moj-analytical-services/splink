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

    sql = sql_gen_block_using_rules(columns, rules, table_name="test")

    db.execute(sql)
    result = db.fetchall()
    result = [dict(r) for r in result]

    correct_answer = [
        {'unique_id_l': 1, 'unique_id_r': 3, 'first_name_l': 'robin', 'first_name_r': 'john', 'surname_l': 'linacre', 'surname_r': 'linacre'},
        {'unique_id_l': 2, 'unique_id_r': 4, 'first_name_l': 'john', 'first_name_r': 'john', 'surname_l': 'smith', 'surname_r': 'smith'},
        {'unique_id_l': 2, 'unique_id_r': 3, 'first_name_l': 'john', 'first_name_r': 'john', 'surname_l': 'smith', 'surname_r': 'linacre'},
        {'unique_id_l': 2, 'unique_id_r': 4, 'first_name_l': 'john', 'first_name_r': 'john', 'surname_l': 'smith', 'surname_r': 'smith'},
        {'unique_id_l': 3, 'unique_id_r': 4, 'first_name_l': 'john', 'first_name_r': 'john', 'surname_l': 'linacre', 'surname_r': 'smith'}
        ]

    pd_correct = pd.DataFrame(correct_answer)
    pd_correct = pd_correct.sort_values(["unique_id_l", "unique_id_r"])
    pd_result = pd.DataFrame(result)
    pd_result = pd_result.sort_values(["unique_id_l", "unique_id_r"])

    assert_frame_equal(pd_correct, pd_result)

