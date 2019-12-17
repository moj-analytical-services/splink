import pandas as pd
import pytest

from sparklink.maximisation_step import sql_gen_new_lambda, sql_gen_intermediate_pi_aggregate

def test_new_lambda(params1, sqlite_con):

    sql = sql_gen_new_lambda(table_name="df_intermediate1")
    df = pd.read_sql(sql, sqlite_con)
    new_lambda = df.iloc[0,0]

    assert new_lambda == pytest.approx(0.611461117)


def test_new_pis(params1, sqlite_con):

    rows = [
        ["gamma_0",0, 0.06188102, 0.417364051],
         ["gamma_0",1, 0.93811898,0.582635949],
          ["gamma_1",0, 0.122657023, 0.321718123],
           ["gamma_1",1, 0.230883807, 0.151396764],
           ["gamma_1",2, 0.64645917, 0.526885114]
    ]



    for r in rows:

        sql = f"""
        select * from df_pi1
         where gamma_col = '{r[0]}'
         and gamma_value = {r[1]}
         """

        df = pd.read_sql(sql, sqlite_con)
        pm = df.loc[0, "new_probability_match"]
        pnm = df.loc[0, "new_probability_non_match"]

        assert pm == pytest.approx(r[2])
        assert pnm == pytest.approx(r[3])
