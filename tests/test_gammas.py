import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from splink.gammas import add_gammas
from pyspark.sql import Row
from pyspark.sql.functions import lit


def test_add_gammas(spark):

    rows = [
        {
            "unique_id_l": 1,
            "unique_id_r": 2,
            "fname_l": "robin",
            "fname_r": "robin",
            "sname_l": "linacre",
            "sname_r": "linacre",
        },
        {
            "unique_id_l": 3,
            "unique_id_r": 4,
            "fname_l": "robin",
            "fname_r": "robin",
            "sname_l": "linacrr",
            "sname_r": "linacre",
        },
        {
            "unique_id_l": 5,
            "unique_id_r": 6,
            "fname_l": None,
            "fname_r": None,
            "sname_l": None,
            "sname_r": "linacre",
        },
        {
            "unique_id_l": 7,
            "unique_id_r": 8,
            "fname_l": "robin",
            "fname_r": "julian",
            "sname_l": "linacre",
            "sname_r": "smith",
        },
    ]

    df = spark.createDataFrame(Row(**x) for x in rows)

    gamma_settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.5,
        "comparison_columns": [
            {"col_name": "fname", "num_levels": 2},
            {
                "col_name": "sname",
                "num_levels": 3,
                "case_expression": """
                                    case
                                    when sname_l is null or sname_r is null then -1
                                    when sname_l = sname_r then 2
                                    when substr(sname_l,1, 3) =  substr(sname_r, 1, 3) then 1
                                    else 0
                                    end
                                    as gamma_sname
                                    """,
            },
        ],
        "blocking_rules": [],
        "retain_matching_columns": False,
    }

    df_gammas = add_gammas(df, gamma_settings, spark)

    correct_answer = [
        {"unique_id_l": 1, "unique_id_r": 2, "gamma_fname": 1, "gamma_sname": 2},
        {"unique_id_l": 3, "unique_id_r": 4, "gamma_fname": 1, "gamma_sname": 1},
        {"unique_id_l": 5, "unique_id_r": 6, "gamma_fname": -1, "gamma_sname": -1},
        {"unique_id_l": 7, "unique_id_r": 8, "gamma_fname": 0, "gamma_sname": 0},
    ]

    pd_correct = pd.DataFrame(correct_answer)
    pd_correct = pd_correct.sort_values(["unique_id_l", "unique_id_r"])
    pd_correct = pd_correct.astype(int)
    pd_result = df_gammas.toPandas()
    pd_result = pd_result.sort_values(["unique_id_l", "unique_id_r"])
    pd_result = pd_result.astype(int)

    assert_frame_equal(pd_correct, pd_result)

    gamma_settings["retain_matching_columns"] = True
    df_gammas = add_gammas(df, gamma_settings, spark)

    result = df_gammas.toPandas()
    col_names = list(result.columns)
    correct_col_names = [
        "unique_id_l",
        "unique_id_r",
        "fname_l",
        "fname_r",
        "gamma_fname",
        "sname_l",
        "sname_r",
        "gamma_sname",
    ]
    assert col_names == correct_col_names

    # With source datset
    gamma_settings["source_dataset_column_name"] = "source_ds"
    df = df.withColumn("source_ds_l", lit("ds"))
    df = df.withColumn("source_ds_r", lit("ds"))

    df_gammas = add_gammas(df, gamma_settings, spark)

    result = df_gammas.toPandas()
    col_names = list(result.columns)
    correct_col_names = [
        "source_ds_l",
        "unique_id_l",
        "source_ds_r",
        "unique_id_r",
        "fname_l",
        "fname_r",
        "gamma_fname",
        "sname_l",
        "sname_r",
        "gamma_sname",
    ]

    assert col_names == correct_col_names
