import pytest
from pyspark.sql import Row
from splink import Splink


def test_nulls(spark):

    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.1,
        "comparison_columns": [
            {
                "col_name": "fname",
                "m_probabilities": [0.4, 0.6],
                "u_probabilities": [0.65, 0.35],
            },
            {
                "col_name": "sname",
                "m_probabilities": [0.25, 0.75],
                "u_probabilities": [0.7, 0.3],
            },
            {
                "col_name": "dob",
                "m_probabilities": [0.4, 0.6],
                "u_probabilities": [0.65, 0.35],
            },
        ],
        "blocking_rules": [],
    }

    rows = [
        {"unique_id": 1, "fname": "Rob", "sname": "Jones", "dob": "1980-01-01"},
        {"unique_id": 2, "fname": "Rob", "sname": "Jones", "dob": None},
        {"unique_id": 3, "fname": "Rob", "sname": None, "dob": None},
        {"unique_id": 4, "fname": None, "sname": None, "dob": None},
    ]

    df = spark.createDataFrame(Row(**x) for x in rows)

    linker = Splink(settings, df, spark)

    df_e = linker.manually_apply_fellegi_sunter_weights()
    df = df_e.toPandas()
    result_list = list(df["match_probability"])

    correct_list = [0.322580645, 0.16, 0.1, 0.16, 0.1, 0.1]

    assert result_list == pytest.approx(correct_list)
