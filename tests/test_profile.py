from pyspark.sql import Row
from splink.profile import _get_top_n, _get_percentiles, _get_df_freq
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

    df_freq = _get_df_freq(df, "col_1", spark)
    top_n = _get_top_n(df_freq, "col_1")

    assert top_n[0]["count"] == 500
    assert top_n[0]["value"] == "robin"
    assert top_n[1]["count"] == 200
    assert top_n[1]["value"] == "john"

    # Percentiles starts with 0th percentile by freq (i.e. lowest freq)
    percentiles = _get_percentiles(df_freq, "col_1")
    # Reverse order
    percentiles = percentiles[::-1]

    assert percentiles[0]["percentile"] == 1.0
    assert percentiles[0]["token_count"] == 500

    assert percentiles[1]["token_count"] == 200
    assert percentiles[-2]["token_count"] == 1

    df_freq = _get_df_freq(df, "col_2", spark, explode_arrays=True)
    top_n = _get_top_n(df_freq, "col_2")
    percentiles = _get_percentiles(df_freq, "col_2")

    assert top_n[0]["count"] == 700
    assert top_n[0]["value"] == "jones"
    assert top_n[1]["count"] == 500
    assert top_n[1]["value"] == "smith"

    percentiles = percentiles[::-1]

    assert percentiles[0]["percentile"] == 1.0
    assert percentiles[0]["token_count"] == 700

    df_freq = _get_df_freq(df, "col_2", spark, explode_arrays=False)
    top_n = _get_top_n(df_freq, "col_2")
    percentiles = _get_percentiles(df_freq, "col_2")

    assert top_n[0]["count"] == 500
    assert top_n[0]["value"] == "smith, jones"
