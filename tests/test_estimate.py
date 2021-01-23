from pyspark.sql import Row
from string import ascii_lowercase

from splink.estimate import estimate_u_values
from uuid import uuid4
from splink.case_statements import sql_gen_case_smnt_strict_equality_2

import pytest


def test_u_estimate(spark):

    # u is the probability of a 'collision' i.e. that amongst non-matches
    # the column will match

    col1 = list(ascii_lowercase[:4] * 5)
    col2 = list(ascii_lowercase[:20])

    zipped = list(zip(col1, col2))

    df1 = [
        {
            "col_1": i[0],
            "col_2": i[1],
            "unique_id": uuid4().hex[:8],
            "source_dataset": "a",
        }
        for i in zipped
    ]

    df2 = [
        {
            "col_1": i[0],
            "col_2": i[1],
            "unique_id": uuid4().hex[:8],
            "source_dataset": "b",
        }
        for i in zipped
    ]

    df1.extend(df2)

    df = spark.createDataFrame(Row(**x) for x in df1)
    df.createOrReplaceTempView("df")

    settings = {
        "comparison_columns": [
            {
                "col_name": "col_1",
                "case_expression": sql_gen_case_smnt_strict_equality_2("col_1"),
            },
            {
                "col_name": "col_2",
                "case_expression": sql_gen_case_smnt_strict_equality_2("col_2"),
            },
        ],
        "link_type": "link_only",
        "unique_id_column_name": "unique_id",
    }

    settings_with_u = estimate_u_values(settings, df, spark)

    u_probs_col_1 = settings_with_u["comparison_columns"][0]["u_probabilities"]

    assert pytest.approx(u_probs_col_1) == [0.75, 0.25]

    u_probs_col_2 = settings_with_u["comparison_columns"][1]["u_probabilities"]

    assert pytest.approx(u_probs_col_2) == [0.95, 0.05]

    # # Check it works for dedupe only
    # No need to run this everytime so commented out

    # df1 = [{"col_1": i[0], "col_2": i[1], "unique_id": uuid4().hex[:8]} for i in zipped]

    # df = spark.createDataFrame(Row(**x) for x in df1)
    # df.createOrReplaceTempView("df")

    # settings = {
    #     "comparison_columns": [
    #         {
    #             "col_name": "col_1",
    #             "case_expression": sql_gen_case_smnt_strict_equality_2("col_1"),
    #         },
    #         {
    #             "col_name": "col_2",
    #             "case_expression": sql_gen_case_smnt_strict_equality_2("col_2"),
    #         },
    #     ],
    #     "link_type": "dedupe_only",
    #     "unique_id_column_name": "unique_id",
    # }

    # settings_with_u = estimate_u_values(settings, df, spark)

    # u_probs_col_1 = settings_with_u["comparison_columns"][0]["u_probabilities"]
    # print(u_probs_col_1)
