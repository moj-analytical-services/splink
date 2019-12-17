import pandas as pd
import pytest

from sparklink.maximisation_step import (
    sql_gen_new_lambda,
    sql_gen_intermediate_pi_aggregate,
)


def test_new_lambda_iteration_2(sqlite_con):
    sql = sql_gen_new_lambda(table_name="df_intermediate1_it2")
    df = pd.read_sql(sql, sqlite_con)
    new_lambda = df.iloc[0, 0]

    assert new_lambda == pytest.approx(0.534993426)


def test_new_pi_iteration_2(sqlite_con):

    rows = [
        ["gamma_0", 0, 0.088546179, 0.435753788],
        ["gamma_0", 1, 0.911453821, 0.564246212],
        ["gamma_1", 0, 0.231340865, 0.27146747],
        ["gamma_1", 1, 0.372351177, 0.109234086],
        ["gamma_1", 2, 0.396307958, 0.619298443]
    ]

    for r in rows:

        sql = f"""
        select * from df_pi1_it2
         where gamma_col = '{r[0]}'
         and gamma_value = {r[1]}
         """

        df = pd.read_sql(sql, sqlite_con)
        pm = df.loc[0, "new_probability_match"]
        pnm = df.loc[0, "new_probability_non_match"]

        assert pm == pytest.approx(r[2])
        assert pnm == pytest.approx(r[3])