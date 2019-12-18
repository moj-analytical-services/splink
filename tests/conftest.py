import pytest
import sqlite3

import pandas as pd

from sparklink.blocking import sql_gen_cartesian_block, sql_gen_block_using_rules
from sparklink.gammas import sql_gen_gammas_case_statement_2_levels, sql_gen_add_gammas, complete_settings_dict
from sparklink.expectation_step import sql_gen_gamma_prob_columns, sql_gen_expected_match_prob
from sparklink.maximisation_step import sql_gen_intermediate_pi_aggregate, sql_gen_pi_df
from sparklink.params import Params


@pytest.fixture(scope='function')
def gamma_settings_1():
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
def params_1(gamma_settings_1):

    # Probability columns
    params = Params(gamma_settings_1, starting_lambda=0.4)
    params.prob_m_2_levels = [0.1, 0.9]  #i.e. 0.1 prob of observing gamma = 0 amongst matches and 0.9 of observing gamma = 1
    params.prob_nm_2_levels = [0.8, 0.2]

    params.prob_m_3_levels = [0.1,0.2,0.7]
    params.prob_nm_3_levels = [0.5,0.25,0.25]
    params.generate_param_dict()
    yield params

@pytest.fixture(scope='function')
def sqlite_con_1(gamma_settings_1, params_1):

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
    df = df.drop_duplicates(["unique_id_l", "unique_id_r"])
    df = df.sort_values(["unique_id_l", "unique_id_r"])
    df.to_sql('df_comparison1', con, index=False)

    sql = sql_gen_add_gammas(gamma_settings_1, include_orig_cols=True, table_name="df_comparison1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_gammas1', con, index=False)

    sql = sql_gen_gamma_prob_columns(params_1, "df_gammas1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_with_gamma_probs1', con, index=False)

    sql = sql_gen_expected_match_prob(params_1, "df_with_gamma_probs1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_with_match_probability1', con, index=False)

    sql = sql_gen_intermediate_pi_aggregate(params_1, table_name="df_with_match_probability1")
    df = pd.read_sql(sql, con)
    df.to_sql('df_intermediate1', con, index=False)

    sql = sql_gen_pi_df(params_1,"df_intermediate1")

    df = pd.read_sql(sql, con)
    df.to_sql('df_pi1', con, index=False)

    # Create a new parameters object and run everything again for a second iteration
     # Probability columns
    params2 = Params(gamma_settings_1, starting_lambda=0.540922141)
    params2.prob_m_2_levels = [0.087438272, 0.912561728]  #i.e. 0.1 prob of observing gamma = 0 amongst matches and 0.9 of observing gamma = 1
    params2.prob_nm_2_levels = [0.441543191, 0.558456809]

    params2.prob_m_3_levels = [0.173315146, 0.326240275, 0.500444578]
    params2.prob_nm_3_levels = [0.340356209, 0.160167628, 0.499476163]
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


@pytest.fixture(scope='function')
def gamma_settings_2():
    gamma_settings = {
    "forename": {
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
    },
    "dob": {
        "levels": 2
    }}

    gamma_settings = complete_settings_dict(gamma_settings)
    yield gamma_settings

@pytest.fixture(scope='function')
def params_2(gamma_settings_2):

    # Probability columns
    params = Params(gamma_settings_2, starting_lambda=0.1)
    params.prob_m_2_levels = [0.4, 0.6]  #i.e. 0.1 prob of observing gamma = 0 amongst matches and 0.9 of observing gamma = 1
    params.prob_nm_2_levels = [0.65, 0.35]

    params.prob_m_3_levels = [0.05,0.2,0.75]
    params.prob_nm_3_levels = [0.4,0.3,0.3]
    params.generate_param_dict()
    yield params



@pytest.fixture(scope='function')
def sqlite_con_2(gamma_settings_2, params_2):

    # Create the database and the database table
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("create table test2 (unique_id, forename, surname, dob)")
    cur.execute("insert into test2 values (?, ?, ?, ?)",  (1, "Robin", "Linacre", "1980-01-01"))
    cur.execute("insert into test2 values (?, ?, ?, ?)",  (2, "Robin", "Linacre", None))
    cur.execute("insert into test2 values (?, ?, ?, ?)",  (3, "Robin", None, None))
    cur.execute("insert into test2 values (?, ?, ?, ?)",  (4, None, None, None))

    sql = "select * from test2 limit 1"
    cur.execute(sql)
    one = cur.fetchone()
    columns = one.keys()

    sql = sql_gen_cartesian_block(columns, table_name="test2")

    df = pd.read_sql(sql, con)
    df = df.sort_values(["unique_id_l", "unique_id_r"])
    df.to_sql('df_comparison2', con, index=False)

    sql = sql_gen_add_gammas(gamma_settings_2, include_orig_cols=True, table_name="df_comparison2")
    df = pd.read_sql(sql, con)
    df.to_sql('df_gammas2', con, index=False)

    sql = sql_gen_gamma_prob_columns(params_2, "df_gammas2")
    df = pd.read_sql(sql, con)
    df.to_sql('df_with_gamma_probs2', con, index=False)

    sql = sql_gen_expected_match_prob(params_2, "df_with_gamma_probs2")
    df = pd.read_sql(sql, con)
    df.to_sql('df_with_match_probability2', con, index=False)

    yield con
