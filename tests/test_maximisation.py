import pandas as pd
import pytest

from sparklink.maximisation_step import (
    sql_gen_new_lambda,
    sql_gen_intermediate_pi_aggregate,
)


def test_new_lambda(params_1, sqlite_con_1):

    sql = sql_gen_new_lambda(table_name="df_intermediate1")
    df = pd.read_sql(sql, sqlite_con_1)
    new_lambda = df.iloc[0, 0]

    assert new_lambda == pytest.approx(0.540922141)


def test_new_pis(params_1, sqlite_con_1):

    rows = [
        ["gamma_0", 0, 0.087438272, 0.441543191],
        ["gamma_0", 1, 0.912561728, 0.558456809],
        ["gamma_1", 0, 0.173315146, 0.340356209],
        ["gamma_1", 1, 0.326240275, 0.160167628],
        ["gamma_1", 2, 0.500444578, 0.499476163],
    ]

    for r in rows:

        sql = f"""
        select * from df_pi1
         where gamma_col = '{r[0]}'
         and gamma_value = {r[1]}
         """

        df = pd.read_sql(sql, sqlite_con_1)
        pm = df.loc[0, "new_probability_match"]
        pnm = df.loc[0, "new_probability_non_match"]

        assert pm == pytest.approx(r[2])
        assert pnm == pytest.approx(r[3])

