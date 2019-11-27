import pytest
from sparklink.gammas import gammas_case_statement_2_levels
import sqlite3

@pytest.fixture(scope='module')
def db():
    # Create the database and the database table
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("create table test (first_name_l, first_name_r)")
    cur.execute("insert into test values (?, ?)", ("robin", "robin"))
    cur.execute("insert into test values (?, ?)", ("robin", "john"))

    yield cur


def test_case(db):

    case_statement = gammas_case_statement_2_levels("first_name", 0)
    sql = f"""select {case_statement} from test"""

    db.execute(sql)
    result = db.fetchall()
    result = [dict(r) for r in result]

    assert result[0]['gamma_0'] == 1
    assert result[1]['gamma_0'] == 0
