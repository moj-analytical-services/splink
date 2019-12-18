import pandas as pd
import pytest

# Need to test null handling is as expected

def test_probability_columns2(sqlite_con_2):
    df = pd.read_sql("select * from df_with_match_probability2", sqlite_con_2)

    result_list = list(df["match_probability"])

    correct_list = [
    0.322580645,
    0.16,
    0.1,
    0.16,
    0.1,
    0.1
    ]

    for i in zip(result_list, correct_list):
        assert i[0] == pytest.approx(i[1])
