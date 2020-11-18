from pyspark.sql import Row

from splink.case_statements import (
    sql_gen_case_stmt_array_intersect_2,
    sql_gen_case_stmt_array_intersect_3,
    sql_gen_case_stmt_array_combinations_leven_3,
    sql_gen_case_stmt_array_combinations_jaro_3,
    sql_gen_case_stmt_array_combinations_jaro_dmeta_4,
)


def test_size_intersection(spark):

    data_list = [
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": ["robin", "john"]},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": ["robin", "james"]},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": ["robyn", "james"]},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": ["rob", "james"]},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": []},
        {"arr_comp_l": ["robin", "john"], "arr_comp_r": None},
    ]

    df = spark.createDataFrame(Row(**x) for x in data_list)
    df.createOrReplaceTempView("df")

    sql = f"""
    select
    {sql_gen_case_stmt_array_intersect_2("arr_comp")} as result_ai2_1,


    {sql_gen_case_stmt_array_intersect_2("arr_comp", zero_length_is_null=False)} as result_ai2_2,


    {sql_gen_case_stmt_array_intersect_3("arr_comp")} as result_ai3,


    {sql_gen_case_stmt_array_combinations_leven_3("arr_comp")} as result_l3_1,

    {sql_gen_case_stmt_array_combinations_leven_3("arr_comp", zero_length_is_null=False)} as result_l3_2,

    {sql_gen_case_stmt_array_combinations_jaro_3("arr_comp")} as result_j3,

    {sql_gen_case_stmt_array_combinations_jaro_dmeta_4("arr_comp")} as result_jd4


    from df
    """

    df_pd = spark.sql(sql).toPandas()

    result = list(df_pd["result_ai2_1"])
    expected = [1, 1, 0, 0, -1, -1]
    assert result == expected

    result = list(df_pd["result_ai2_2"])
    expected = [1, 1, 0, 0, 0, -1]
    assert result == expected

    result = list(df_pd["result_ai3"])
    expected = [2, 1, 0, 0, -1, -1]
    assert result == expected

    result = list(df_pd["result_l3_1"])
    expected = [2, 2, 2, 1, -1, -1]
    assert result == expected

    result = list(df_pd["result_l3_2"])
    expected = [2, 2, 2, 1, 0, -1]
    assert result == expected

    result = list(df_pd["result_j3"])
    expected = [2, 2, 1, 1, -1, -1]
    assert result == expected

    result = list(df_pd["result_jd4"])
    expected = [3, 3, 2, 1, -1, -1]
    assert result == expected