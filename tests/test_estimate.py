from pyspark.sql import Row
from string import ascii_lowercase

from splink.estimate import estimate_u_values
from splink_data_generation.generate_data_exact import generate_df_gammas_exact
from uuid import uuid4
from splink.case_statements import sql_gen_case_smnt_strict_equality_2


def test_u_estimate(spark):

    # u is the probability of a 'collision' i.e. that amongst non-matches
    # the column will match

    col1 = list(ascii_lowercase[:4] * 5)
    col2 = list(ascii_lowercase[:20])

    zipped = zip(col1, col2)

    data_list = [
        {"col_1": i[0], "col_2": i[1], "unique_id": uuid4().hex[:8]} for i in zipped
    ]

    df = spark.createDataFrame(Row(**x) for x in data_list)
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
        "link_type": "dedupe_only",
        "unique_id_column_name": "unique_id",
    }

    set = estimate_u_values(settings, df, 1e10, spark)
    print(set)