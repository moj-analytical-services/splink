# Not much point in doing full test coverage of this until we're confident the formulas we're implementing
# are the correct ones.
# See discussion here:
# https://github.com/moj-analytical-services/sparklink/issues/17

# Basically, only want to do full test coverage here after we've checked our results align with R fasklink pacakge.

import pytest
import sqlite3
import pandas as pd

from sparklink.term_frequencies import *

def data_into_table(data, table_name, con):
    cur = con.cursor()

    keys = data[0].keys()
    cols = ", ".join(keys)

    sql = f"""
    create table if not exists {table_name} ({cols})
    """

    cur.execute(sql)

    question_marks = ", ".join("?" for k in keys)
    insert_statement = f"insert into {table_name} values ({question_marks})"

    for d in data:
        values = tuple(d.values())
        cur.execute(insert_statement, values)


@pytest.fixture(scope='function')
def tf_test(gamma_settings_1, params_1):

     # Create the database and the database table
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    data = [
    {"surname_l": "Smith", "surname_r": "Smith", "fname_l": "John", "fname_r": "John", "match_probability": 0.1},
    {"surname_l": "Smith", "surname_r": "Smith", "fname_l": "John", "fname_r": "John", "match_probability": 0.1},
    {"surname_l": "Linacre", "surname_r": "Linacre", "fname_l": "Robin", "fname_r": "Robin", "match_probability": 0.7},
    {"surname_l": "Jones", "surname_r": "Jones", "fname_l": "James", "fname_r": "David", "match_probability": 0.2},
    {"surname_l": "Johnston", "surname_r": "May", "fname_l": "David", "fname_r": "David", "match_probability": 0.3},

    ]

    data_into_table(data, "test_freq", con)

    yield con




def test_expected_match_prob(tf_test):

    class Test_Params:
        def __init__(self):
            self.params = {}
            self.params["λ"] = 0.5


    params = Test_Params()
    sql = sql_gen_generate_adjusted_lambda("surname", params, table_name='test_freq')

    df = pd.read_sql(sql, tf_test)
