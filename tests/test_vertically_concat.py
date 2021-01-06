from splink.vertically_concat import vertically_concatenate_datasets

from pyspark.sql import Row

import pytest


def test_vertically_concat(spark):
    data_list_1 = [{"a": 1, "b": 1}, {"a": 2, "b": 2}]
    data_list_2 = [{"a": 3, "b": 3}, {"a": 4, "b": 4}]
    data_list_3 = [{"b": 1, "a": 10}, {"b": 2, "a": 20}]
    data_list_4 = [{"b": 1, "a": 10, "c": 1}, {"b": 2, "a": 20, "c": 2}]
    data_list_5 = [{"a": "a", "b": 1}, {"a": "b", "b": 2}]

    df1 = spark.createDataFrame(Row(**x) for x in data_list_1)
    df2 = spark.createDataFrame(Row(**x) for x in data_list_2)
    df3 = spark.createDataFrame(Row(**x) for x in data_list_3)
    df4 = spark.createDataFrame(Row(**x) for x in data_list_4)
    df5 = spark.createDataFrame(Row(**x) for x in data_list_5)

    df = vertically_concatenate_datasets([df1, df2, df3, df1])

    dfpd = df.toPandas()
    dfpd.sort_values(["a", "b"])

    assert list(dfpd["a"]) == [1, 2, 3, 4, 10, 20, 1, 2]
    assert list(dfpd["b"]) == [1, 2, 3, 4, 1, 2, 1, 2]

    # Error because different columns
    with pytest.raises(ValueError):
        vertically_concatenate_datasets([df4, df2]).show()

    # Error because different types
    with pytest.raises(ValueError):
        vertically_concatenate_datasets([df1, df5]).show()
