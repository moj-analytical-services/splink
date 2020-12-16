from pyspark.sql import Row
from splink.profile import (
    _generate_df_all_column_value_frequencies,
    _generate_df_all_column_value_frequencies_array,
    _get_df_percentiles,
    _get_df_top_bottom_n,
    _collect_and_group_percentiles_df,
    _collect_and_group_top_values,
)
from uuid import uuid4


def test_top_n(spark):

    data_list = []

    for i in range(500):
        data_list.append({"col_1": "robin", "col_2": ["smith", "jones"]})

    for i in range(200):
        data_list.append({"col_1": "john", "col_2": ["jones"]})

    for i in range(300):
        data_list.append(
            {
                "col_1": uuid4().hex[:10],
                "col_2": [uuid4().hex[:10], uuid4().hex[:10], uuid4().hex[:10]],
            }
        )

    df = spark.createDataFrame(Row(**x) for x in data_list)
    df.createOrReplaceTempView("df")

    df_acvf = _generate_df_all_column_value_frequencies(["col_1", "col_2"], df, spark)
    df_acvf.createOrReplaceTempView("df_acvf")
    df_acvf = df_acvf.persist()

    df_perc = _get_df_percentiles(df_acvf, spark)
    df_top_n = _get_df_top_bottom_n(df_acvf, spark, 20)

    percentiles_collected = _collect_and_group_percentiles_df(df_perc)
    top_n_collected = _collect_and_group_top_values(df_top_n)

    percentiles = percentiles_collected["col_1"]
    top_n = top_n_collected["col_1"]

    assert top_n[0]["value_count"] == 500
    assert top_n[0]["value"] == "robin"
    assert top_n[1]["value_count"] == 200
    assert top_n[1]["value"] == "john"

    assert percentiles[0]["percentile_ex_nulls"] == 1.0
    assert percentiles[0]["value_count"] == 500

    assert percentiles[1]["value_count"] == 500
    assert percentiles[2]["value_count"] == 200
    assert percentiles[-1]["value_count"] == 1

    percentiles = percentiles_collected["col_2"]
    top_n = top_n_collected["col_2"]

    assert top_n[0]["value_count"] == 500
    assert top_n[0]["value"] == "smith, jones"

    df_acvf = _generate_df_all_column_value_frequencies_array(["col_2"], df, spark)
    df_acvf.createOrReplaceTempView("df_acvf")
    df_acvf = df_acvf.persist()

    df_top_n = _get_df_top_bottom_n(df_acvf, spark, 20)

    top_n = _collect_and_group_top_values(df_top_n)["col_2"]

    assert top_n[0]["value_count"] == 700
    assert top_n[0]["value"] == "jones"
