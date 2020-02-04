import sqlite3

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from sparklink.case_statements import *
from sparklink.case_statements import _add_null_treatment_to_case_statement




def test_case(sqlite_con_3):

    case_statement = sql_gen_case_smnt_strict_equality_2("str_col", 0)
    sql = f"""select {case_statement} from str_comp"""

    sqlite_con_3.execute(sql)
    result = sqlite_con_3.fetchall()
    result = [dict(r) for r in result]

    assert result[0]['gamma_0'] == 1
    assert result[1]['gamma_0'] == 0
    assert result[2]['gamma_0'] == 0

    sql = """
    select case  when str_col_l = str_col_r then 2
    when str_col_l = 'hi' then 1
    else 0 end as gamma_0 from str_comp
    """

    sql = _add_null_treatment_to_case_statement(sql)

    sqlite_con_3.execute(sql)
    result = sqlite_con_3.fetchall()
    result = [dict(r) for r in result]

    assert result[0]['gamma_0'] == 2
    assert result[1]['gamma_0'] == 0
    assert result[2]['gamma_0'] == 0
    assert result[3]['gamma_0'] == -1
    assert result[4]['gamma_0'] == -1

    case_statement = sql_gen_case_stmt_numeric_abs_3("float_col", 1, 0)
    sql = f"""select {case_statement} from float_comp"""

    sqlite_con_3.execute(sql)
    result = sqlite_con_3.fetchall()
    result = [dict(r) for r in result]


    assert result[0]['gamma_0'] == 2
    assert result[1]['gamma_0'] == 1
    assert result[2]['gamma_0'] == 0
    assert result[3]['gamma_0'] == 0
    assert result[4]['gamma_0'] == -1


    case_statement = sql_gen_case_stmt_numeric_abs_4("float_col", 1, 10, 0)
    sql = f"""select {case_statement} from float_comp"""

    sqlite_con_3.execute(sql)
    result = sqlite_con_3.fetchall()
    result = [dict(r) for r in result]

    assert result[0]['gamma_0'] == 3
    assert result[1]['gamma_0'] == 2
    assert result[2]['gamma_0'] == 1
    assert result[3]['gamma_0'] == 0
    assert result[4]['gamma_0'] == -1


    case_statement = sql_gen_case_stmt_numeric_perc_3("float_col", 0.01, 0)
    sql = f"""select {case_statement} from float_comp"""

    sqlite_con_3.execute(sql)
    result = sqlite_con_3.fetchall()
    result = [dict(r) for r in result]


    assert result[0]['gamma_0'] == 2
    assert result[1]['gamma_0'] == 1
    assert result[2]['gamma_0'] == 0
    assert result[3]['gamma_0'] == 0
    assert result[4]['gamma_0'] == -1


    case_statement = sql_gen_case_stmt_numeric_perc_3("float_col", 0.20, 0)
    sql = f"""select {case_statement} from float_comp"""

    sqlite_con_3.execute(sql)
    result = sqlite_con_3.fetchall()
    result = [dict(r) for r in result]


    assert result[0]['gamma_0'] == 2
    assert result[1]['gamma_0'] == 1
    assert result[2]['gamma_0'] == 1
    assert result[3]['gamma_0'] == 1
    assert result[4]['gamma_0'] == -1

    case_statement = sql_gen_case_stmt_numeric_perc_4("float_col", 0.01, 0.1, 0)
    sql = f"""select {case_statement} from float_comp"""
    print(sql)
    sqlite_con_3.execute(sql)
    result = sqlite_con_3.fetchall()
    result = [dict(r) for r in result]

    assert result[0]['gamma_0'] == 3
    assert result[1]['gamma_0'] == 2
    assert result[2]['gamma_0'] == 1
    assert result[3]['gamma_0'] == 0
    assert result[4]['gamma_0'] == -1

