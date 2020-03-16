import sqlite3

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from splink.gammas import _sql_gen_add_gammas
from splink.settings import sql_gen_case_smnt_strict_equality_2, complete_settings_dict
from splink.expectation_step import _sql_gen_gamma_prob_columns, _sql_gen_expected_match_prob
from splink.params import Params

def test_probability_columns(sqlite_con_1, gamma_settings_1):


    params = Params(gamma_settings_1, spark="supress_warnings")

    sql = _sql_gen_gamma_prob_columns(params, gamma_settings_1,"df_gammas1")
    df = pd.read_sql(sql, sqlite_con_1)

    cols_to_keep = ["prob_gamma_mob_match", "prob_gamma_mob_non_match", "prob_gamma_surname_match", "prob_gamma_surname_non_match"]
    pd_df_result = df[cols_to_keep][:4]

    df_correct = [{"prob_gamma_mob_match": 0.9,
     "prob_gamma_mob_non_match": 0.2,
     "prob_gamma_surname_match": 0.7,
     "prob_gamma_surname_non_match": 0.25
    },
    {"prob_gamma_mob_match": 0.9,
     "prob_gamma_mob_non_match": 0.2,
     "prob_gamma_surname_match": 0.2,
     "prob_gamma_surname_non_match": 0.25
    },
    {"prob_gamma_mob_match": 0.9,
     "prob_gamma_mob_non_match": 0.2,
     "prob_gamma_surname_match": 0.2,
     "prob_gamma_surname_non_match": 0.25
    },
    {"prob_gamma_mob_match": 0.1,
     "prob_gamma_mob_non_match": 0.8,
     "prob_gamma_surname_match": 0.7,
     "prob_gamma_surname_non_match": 0.25
    }]

    pd_df_correct = pd.DataFrame(df_correct)

    assert_frame_equal(pd_df_correct, pd_df_result)

def test_expected_match_prob(gamma_settings_1, params_1, sqlite_con_1):

    df = pd.read_sql("select * from df_with_match_probability1", sqlite_con_1)


    sql = _sql_gen_expected_match_prob(params_1, gamma_settings_1, "df_with_gamma_probs1")
    df = pd.read_sql(sql, sqlite_con_1)
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

