import sqlite3

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from sparklink.gammas import sql_gen_gammas_case_statement_2_levels, sql_gen_add_gammas, complete_settings_dict
from sparklink.expectation_step import sql_gen_gamma_prob_columns, sql_gen_expected_match_prob
from sparklink.params import Params


def test_probability_columns(sqlite_con):

    gamma_settings = {
    "mob": {
        "levels": 2
    },
    "surname": {
        "levels": 3
    }
    }

    params = Params(gamma_settings, starting_lambda=0.4)
    params.prob_m_2_levels = [0.1, 0.9]
    params.prob_nm_2_levels = [0.8, 0.2]

    params.prob_m_3_levels = [0.1,0.2,0.7]
    params.prob_nm_3_levels = [0.5,0.25,0.25]
    params.generate_param_dict()

    sql = sql_gen_gamma_prob_columns(params, "df_gammas1")
    df = pd.read_sql(sql, sqlite_con)

    cols_to_keep = ["prob_gamma_0_match", "prob_gamma_0_non_match", "prob_gamma_1_match", "prob_gamma_1_non_match"]
    pd_df_result = df[cols_to_keep][:4]

    df_correct = [{"prob_gamma_0_match": 0.9,
     "prob_gamma_0_non_match": 0.2,
     "prob_gamma_1_match": 0.7,
     "prob_gamma_1_non_match": 0.25
    },
    {"prob_gamma_0_match": 0.9,
     "prob_gamma_0_non_match": 0.2,
     "prob_gamma_1_match": 0.2,
     "prob_gamma_1_non_match": 0.25
    },
    {"prob_gamma_0_match": 0.9,
     "prob_gamma_0_non_match": 0.2,
     "prob_gamma_1_match": 0.2,
     "prob_gamma_1_non_match": 0.25
    },
    {"prob_gamma_0_match": 0.1,
     "prob_gamma_0_non_match": 0.8,
     "prob_gamma_1_match": 0.7,
     "prob_gamma_1_non_match": 0.25
    }]

    pd_df_correct = pd.DataFrame(df_correct)

    assert_frame_equal(pd_df_correct, pd_df_result)

def test_expected_match_prob(params1, sqlite_con):

    df = pd.read_sql("select * from df_with_match_probability1", sqlite_con)


    cols_to_keep = ["prob_gamma_0_match", "prob_gamma_0_non_match", "prob_gamma_1_match", "prob_gamma_1_non_match"]

    sql = sql_gen_expected_match_prob(params1, "df_with_gamma_probs1")
    df = pd.read_sql(sql, sqlite_con)
    result_list = list(df["match_probability"])

    correct_list = [
    0.893617021,
    0.705882353,
    0.705882353,
    0.189189189,
    0.189189189,
    0.893617021,
    0.375,
    0.375]

    for i in zip(result_list, correct_list):
        assert i[0] == pytest.approx(i[1])
