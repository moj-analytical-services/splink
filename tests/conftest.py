import pytest
import sqlite3

import pandas as pd
import copy

from splink.blocking import sql_gen_cartesian_block, sql_gen_block_using_rules
from splink.gammas import sql_gen_add_gammas, complete_settings_dict
from splink.expectation_step import (
    _sql_gen_gamma_prob_columns,
    _sql_gen_expected_match_prob,
)
from splink.maximisation_step import _sql_gen_intermediate_pi_aggregate, _sql_gen_pi_df
from splink.params import Params
from splink.case_statements import sql_gen_case_smnt_strict_equality_2


@pytest.mark.filterwarnings("ignore:*")
@pytest.fixture(scope="function")
def gamma_settings_1():
    gamma_settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.4,
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.8, 0.2],
            },
            {
                "col_name": "surname",
                "num_levels": 3,
                "case_expression": """
            case
            when surname_l = surname_r then 2
            when substr(surname_l,1, 3) =  substr(surname_r, 1, 3) then 1
            else 0
            end
            as gamma_surname
            """,
                "m_probabilities": [0.1, 0.2, 0.7],
                "u_probabilities": [0.5, 0.25, 0.25],
            },
        ],
        "blocking_rules": []
    }
    gamma_settings = complete_settings_dict(gamma_settings, spark="supress_warnings")
    yield gamma_settings


@pytest.mark.filterwarnings("ignore:*")
@pytest.fixture(scope="function")
def params_1(gamma_settings_1):

    # Probability columns
    params = Params(gamma_settings_1, spark="supress_warnings")
    yield params


@pytest.fixture(scope="function")
def sqlite_con_1(gamma_settings_1, params_1):

    # Create the database and the database table
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("create table test1 (unique_id, mob, surname)")
    cur.execute("insert into test1 values (?, ?, ?)", (1, 10, "Linacre"))
    cur.execute("insert into test1 values (?, ?, ?)", (2, 10, "Linacre"))
    cur.execute("insert into test1 values (?, ?, ?)", (3, 10, "Linacer"))
    cur.execute("insert into test1 values (?, ?, ?)", (4, 7, "Smith"))
    cur.execute("insert into test1 values (?, ?, ?)", (5, 8, "Smith"))
    cur.execute("insert into test1 values (?, ?, ?)", (6, 8, "Smith"))
    cur.execute("insert into test1 values (?, ?, ?)", (7, 8, "Jones"))

    # Create comparison table
    rules = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    sql = "select * from test1 limit 1"
    cur.execute(sql)
    one = cur.fetchone()
    columns = one.keys()

    sql = sql_gen_block_using_rules("dedupe_only", columns, rules, table_name_dedupe="test1")
    df = pd.read_sql(sql, con)
    df = df.drop_duplicates(["unique_id_l", "unique_id_r"])
    df = df.sort_values(["unique_id_l", "unique_id_r"])
    df.to_sql("df_comparison1", con, index=False)

    sql = sql_gen_add_gammas(
        gamma_settings_1, table_name="df_comparison1"
    )

    df = pd.read_sql(sql, con)
    df.to_sql("df_gammas1", con, index=False)

    sql = _sql_gen_gamma_prob_columns(params_1, gamma_settings_1, "df_gammas1")
    df = pd.read_sql(sql, con)
    df.to_sql("df_with_gamma_probs1", con, index=False)

    sql = _sql_gen_expected_match_prob(params_1, gamma_settings_1, "df_with_gamma_probs1")
    df = pd.read_sql(sql, con)
    df.to_sql("df_with_match_probability1", con, index=False)

    sql = _sql_gen_intermediate_pi_aggregate(
        params_1, table_name="df_with_match_probability1"
    )
    df = pd.read_sql(sql, con)
    df.to_sql("df_intermediate1", con, index=False)

    sql = _sql_gen_pi_df(params_1, "df_intermediate1")

    df = pd.read_sql(sql, con)
    df.to_sql("df_pi1", con, index=False)

    # Create a new parameters object and run everything again for a second iteration
    # Probability columns
    gamma_settings_it_2 = copy.deepcopy(gamma_settings_1)
    gamma_settings_it_2["proportion_of_matches"] = 0.540922141
    gamma_settings_it_2["comparison_columns"][0]["m_probabilities"] = [0.087438272, 0.912561728]
    gamma_settings_it_2["comparison_columns"][0]["u_probabilities"] = [0.441543191, 0.558456809]
    gamma_settings_it_2["comparison_columns"][1]["m_probabilities"] = [
        0.173315146,
        0.326240275,
        0.500444578,
    ]
    gamma_settings_it_2["comparison_columns"][1]["u_probabilities"] = [
        0.340356209,
        0.160167628,
        0.499476163,
    ]

    params2 = Params(
        gamma_settings_it_2, spark="supress_warnings"
    )

    params2._generate_param_dict()

    sql = _sql_gen_gamma_prob_columns(params2, gamma_settings_it_2, "df_gammas1")
    df = pd.read_sql(sql, con)
    df.to_sql("df_with_gamma_probs1_it2", con, index=False)

    sql = _sql_gen_expected_match_prob(params2, gamma_settings_it_2, "df_with_gamma_probs1_it2")
    df = pd.read_sql(sql, con)
    df.to_sql("df_with_match_probability1_it2", con, index=False)

    sql = _sql_gen_intermediate_pi_aggregate(
        params2, table_name="df_with_match_probability1_it2"
    )
    df = pd.read_sql(sql, con)
    df.to_sql("df_intermediate1_it2", con, index=False)

    sql = _sql_gen_pi_df(params2, "df_intermediate1_it2")

    df = pd.read_sql(sql, con)
    df.to_sql("df_pi1_it2", con, index=False)

    yield con


@pytest.mark.filterwarnings("ignore:*")
@pytest.fixture(scope="function")
def gamma_settings_2():
    gamma_settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.1,
        "comparison_columns": [
            {
                "col_name": "forename",
                "num_levels": 2,
                "m_probabilities": [0.4, 0.6],
                "u_probabilities": [0.65, 0.35],
            },
            {
                "col_name": "surname",
                "num_levels": 3,
                "case_expression": """
        case
        when surname_l = surname_r then 2
        when substr(surname_l,1, 3) =  substr(surname_r, 1, 3) then 1
        else 0
        end
        as gamma_surname
        """,
                "m_probabilities": [0.05, 0.2, 0.75],
                "u_probabilities": [0.4, 0.3, 0.3],
            },
            {
                "col_name": "dob",
                "num_levels": 2,
                "m_probabilities": [0.4, 0.6],
                "u_probabilities": [0.65, 0.35],
            },
        ],
        "blocking_rules": []
    }

    gamma_settings = complete_settings_dict(gamma_settings, spark="supress_warnings")
    yield gamma_settings


@pytest.fixture(scope="function")
def params_2(gamma_settings_2):

    # Probability columns
    params = Params(gamma_settings_2, spark="supress_warnings")

    params._generate_param_dict()
    yield params


@pytest.fixture(scope="function")
def sqlite_con_2(gamma_settings_2, params_2):

    # Create the database and the database table
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("create table test2 (unique_id, forename, surname, dob)")
    cur.execute(
        "insert into test2 values (?, ?, ?, ?)", (1, "Robin", "Linacre", "1980-01-01")
    )
    cur.execute("insert into test2 values (?, ?, ?, ?)", (2, "Robin", "Linacre", None))
    cur.execute("insert into test2 values (?, ?, ?, ?)", (3, "Robin", None, None))
    cur.execute("insert into test2 values (?, ?, ?, ?)", (4, None, None, None))

    sql = "select * from test2 limit 1"
    cur.execute(sql)
    one = cur.fetchone()
    columns = one.keys()

    sql = sql_gen_cartesian_block(
        link_type="dedupe_only", columns_to_retain=columns, table_name_dedupe="test2"
    )

    df = pd.read_sql(sql, con)
    df = df.sort_values(["unique_id_l", "unique_id_r"])
    df.to_sql("df_comparison2", con, index=False)

    sql = sql_gen_add_gammas(
        gamma_settings_2, table_name="df_comparison2"
    )
    df = pd.read_sql(sql, con)
    df.to_sql("df_gammas2", con, index=False)

    sql = _sql_gen_gamma_prob_columns(params_2, gamma_settings_2,"df_gammas2")
    df = pd.read_sql(sql, con)
    df.to_sql("df_with_gamma_probs2", con, index=False)

    sql = _sql_gen_expected_match_prob(params_2, gamma_settings_2, "df_with_gamma_probs2")
    df = pd.read_sql(sql, con)
    df.to_sql("df_with_match_probability2", con, index=False)

    yield con


@pytest.fixture(scope="function")
def sqlite_con_3():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("create table str_comp (str_col_l, str_col_r)")
    cur.execute(
        "insert into str_comp values (?, ?)",
        ("these strings are equal", "these strings are equal"),
    )
    cur.execute(
        "insert into str_comp values (?, ?)",
        ("these strings are almost equal", "these strings are almos equal"),
    )
    cur.execute(
        "insert into str_comp values (?, ?)",
        ("these strings are almost equal", "not the same at all"),
    )
    cur.execute(
        "insert into str_comp values (?, ?)", ("these strings are almost equal", None)
    )
    cur.execute("insert into str_comp values (?, ?)", (None, None))

    cur.execute("create table float_comp (float_col_l, float_col_r)")
    cur.execute("insert into float_comp values (?, ?)", (1.0, 1.0))
    cur.execute("insert into float_comp values (?, ?)", (100.0, 99.9))
    cur.execute("insert into float_comp values (?, ?)", (100.0, 90.1))
    cur.execute("insert into float_comp values (?, ?)", (-100.0, -85.1))
    cur.execute("insert into float_comp values (?, ?)", (None, -85.1))

    yield con


# Generate a test dataset with known data generating process to see if
# results iterate towards the 'right answer'


@pytest.fixture(scope="function")
def gamma_settings_4():
    gamma_settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches":0.9,
        "comparison_columns": [
            {
                "col_name": "col_2_levels",
                "num_levels": 2,
                "case_expression": sql_gen_case_smnt_strict_equality_2("col_2_levels"),
            },
            {
                "col_name": "col_5_levels",
                "num_levels": 2,
                "case_expression": sql_gen_case_smnt_strict_equality_2("col_5_levels"),
            },
            {
                "col_name": "col_20_levels",
                "num_levels": 2,
                "case_expression": sql_gen_case_smnt_strict_equality_2("col_20_levels"),
            },
        ],
        "blocking_rules": []
    }

    gamma_settings = complete_settings_dict(gamma_settings, spark="supress_warnings")
    yield gamma_settings


@pytest.fixture(scope="function")
def params_4(gamma_settings_4):

    # Probability columns
    params = Params(gamma_settings_4, spark="supress_warnings")

    params._generate_param_dict()
    yield params


@pytest.fixture(scope="function")
def sqlite_con_4(gamma_settings_4):

    ## Going to create all combinatinos of gammas in the right frequencies to guarantee independence

    col_names = [c["col_name"] for c in gamma_settings_4["comparison_columns"]]
    ## Create df gammas for non-matches
    probs = [
        0.05,
        0.2,
        0.5,
    ]  # Amongst non-matches, gamma_0 agrees 5% of the time, gamma_1 agrees 20% of the time etc
    iprobs = [1 / p for p in probs]

    df_nm = None

    for index, num_options in enumerate(iprobs):
        col_name = col_names[index]
        n = int(num_options)
        df_nm_new = pd.DataFrame(
            {f"gamma_{col_name}": [0] * (n - 1) + [1], "join_col": [1] * n}
        )  # Creates n rec
        if df_nm is not None:
            df_nm = df_nm.merge(df_nm_new, left_on="join_col", right_on="join_col")

        else:
            df_nm = df_nm_new
    df_nm = df_nm.drop("join_col", axis=1)
    df_nm["true_match"] = 0

    ## Create df gammas for non-matches
    probs = [
        0.05,
        0.1,
        0.05,
    ]  # Amongst matches, gamma_0 DISAGREES 5% of the time, gamma_1 DISAGREES 10% of the time etc
    iprobs = [1 / p for p in probs]

    df_m = None
    for index, num_options in enumerate(iprobs):
        col_name = col_names[index]
        n = int(num_options)
        df_m_new = pd.DataFrame(
            {f"gamma_{col_name}": [1] * (n - 1) + [0], "join_col": [1] * n}
        )
        if df_m is not None:
            df_m = df_m.merge(df_m_new, left_on="join_col", right_on="join_col")

        else:
            df_m = df_m_new
    df_m = df_m.drop("join_col", axis=1)
    df_m["true_match"] = 1

    df_all = pd.concat([df_nm, df_m])
    df_all = df_all.reset_index()
    df_all = df_all.rename(columns = {"index": "unique_id_l"})
    df_all["unique_id_r"] = df_all["unique_id_l"]

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    df_all.to_sql("df", con, index=False)



    yield con
