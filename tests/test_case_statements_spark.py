from pyspark.sql import Row

from splink.case_statements import (
    sql_gen_case_stmt_array_intersect_2,
    sql_gen_case_stmt_array_intersect_3,
)


def test_size_intersection(spark):

    data_list = [
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": ["robin", "john"]},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": ["robin", "james"]},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": ["robyn", "james"]},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": []},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": None},
    ]

    df = spark.createDataFrame(Row(**x) for x in data_list)
    df.createOrReplaceTempView("df")

    sql = f"""
    select
    {sql_gen_case_stmt_array_intersect_2("arr_comp")} as result1a,
    {sql_gen_case_stmt_array_intersect_2("arr_comp", zero_length_is_null=False)} as result1b,
    {sql_gen_case_stmt_array_intersect_3("arr_comp")} as result2
    from df
    """

    df_pd = spark.sql(sql).toPandas()

    result = list(df_pd["result1a"])
    expected = [1, 1, 0, -1, -1]
    assert result == expected

    result = list(df_pd["result1b"])
    expected = [1, 1, 0, 0, -1]
    assert result == expected

    result = list(df_pd["result2"])
    expected = [2, 1, 0, -1, -1]
    assert result == expected
