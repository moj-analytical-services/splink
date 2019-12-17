import pytest
import sqlite3

import pandas as pd

from sparklink.blocking import sql_gen_block_using_rules
from sparklink.gammas import sql_gen_gammas_case_statement_2_levels, sql_gen_add_gammas, complete_settings_dict
from sparklink.expectation_step import sql_gen_gamma_prob_columns, sql_gen_expected_match_prob
from sparklink.maximisation_step import sql_gen_intermediate_pi_aggregate, sql_gen_pi_df
from sparklink.params import Params


@pytest.fixture(scope='function')
def gamma_settings1():
    gamma_settings = {
    "mob": {
        "levels": 2
    },
    "surname": {
        "levels": 3,
        "case_expression": """
        case
        when surname_l = surname_r then 2
        when substr(surname_l,1, 3) =  substr(surname_r, 1, 3) then 1
        else 0
        end
        as gamma_1
        """
    }}
    gamma_settings = complete_settings_dict(gamma_settings)
    yield gamma_settings

@pytest.fixture(scope='function')
def params1(gamma_settings1):

    # Probability columns
    params = Params(gamma_settings1, starting_lambda=0.4)
    params.prob_m_2_levels = [0.1, 0.9]  #i.e. 0.1 prob of observing gamma = 0 amongst matches and 0.9 of observing gamma = 1
    params.prob_nm_2_levels = [0.8, 0.2]

    params.prob_m_3_levels = [0.1,0.2,0.7]
    params.prob_nm_3_levels = [0.5,0.25,0.25]
    params.generate_param_dict()
    yield params

@pytest.fixture(scope='function')
def sqlite_con(gamma_settings1, params1):
    # Create the database and the database table
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("create table test1 (unique_id, mob, surname)")
    cur.execute("insert into test1 values (?, ?, ?)",  (1, 10, "Linacre"))
    cur.execute("insert into test1 values (?, ?, ?)",  (2, 10, "Linacre"))
    cur.execute("insert into test1 values (?, ?, ?)",  (3, 10, "Linacer"))
    cur.execute("insert into test1 values (?, ?, ?)",  (4, 7, "Smith"))
    cur.execute("insert into test1 values (?, ?, ?)",  (5, 8, "Smith"))
    cur.execute("insert into test1 values (?, ?, ?)",  (6, 8, "Smith"))
    cur.execute("insert into test1 values (?, ?, ?)",  (7, 8, "Jones"))

    # Create comparison table
    rules = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    sql = "select * from test1 limit 1"
    cur.execute(sql)
    one = cur.fetchone()
    columns = one.keys()

    sql = sql_gen_block_using_rules(columns, rules, table_name="test1")
    df = pd.read_sql(sql, con)
    df = df.sort_values(["unique_id_l", "unique_id_r"])
    df.to_sql('df_comparison1', con, index=False)

    sql = sql_gen_add_gammas(gamma_settings1, include_orig_cols=True, table_name="df_comparison1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_gammas1', con, index=False)

    sql = sql_gen_gamma_prob_columns(params1, "df_gammas1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_with_gamma_probs1', con, index=False)

    sql = sql_gen_expected_match_prob(params1, "df_with_gamma_probs1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_with_match_probability1', con, index=False)

    sql = sql_gen_intermediate_pi_aggregate(params1, table_name="df_with_match_probability1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_intermediate1', con, index=False)

    sql = sql_gen_pi_df(params1,"df_intermediate1")

    df = pd.read_sql(sql, con)
    df.to_sql('df_pi1', con, index=False)

    # Create a new parameters object and run everything again.

     # Probability columns
    params2 = Params(gamma_settings1, starting_lambda=0.611461117)
    params2.prob_m_2_levels = [0.06188102, 0.93811898]  #i.e. 0.1 prob of observing gamma = 0 amongst matches and 0.9 of observing gamma = 1
    params2.prob_nm_2_levels = [0.417364051, 0.582635949]

    params2.prob_m_3_levels = [0.122657023, 0.230883807, 0.64645917]
    params2.prob_nm_3_levels = [0.321718123, 0.151396764, 0.526885114]
    params2.generate_param_dict()

    sql = sql_gen_gamma_prob_columns(params2, "df_gammas1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_with_gamma_probs1_it2', con, index=False)

    sql = sql_gen_expected_match_prob(params2, "df_with_gamma_probs1_it2")
    df = pd.read_sql(sql, con)
    df.to_sql('df_with_match_probability1_it2', con, index=False)

    sql = sql_gen_intermediate_pi_aggregate(params2, table_name="df_with_match_probability1_it2")
    df = pd.read_sql(sql, con)
    df.to_sql('df_intermediate1_it2', con, index=False)

    sql = sql_gen_pi_df(params2,"df_intermediate1_it2")

    df = pd.read_sql(sql, con)
    df.to_sql('df_pi1_it2', con, index=False)


    yield con