from pyspark.sql import Row

from splink.case_statements import (
    sql_gen_case_stmt_array_intersect_2,
    sql_gen_case_stmt_array_intersect_3,
    sql_gen_case_stmt_array_combinations_leven_3,
    sql_gen_case_stmt_array_combinations_jaro_3,
    sql_gen_case_stmt_array_combinations_jaro_dmeta_4,
    sql_gen_gammas_name_inversion_4,
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


def test_name_inversion(spark):

    data_list = [
        {
            "name_1_l": "john",
            "name_1_r": "john",
            "name_2_l": "richard",
            "name_2_r": "richard",
            "name_3_l": "michael",
            "name_3_r": "michael",
            "name_4_l": "smith",
            "name_4_r": "smith",
        },
        {
            "name_1_l": "richard",
            "name_1_r": "john",
            "name_2_l": "john",
            "name_2_r": "richard",
            "name_3_l": "michael",
            "name_3_r": "michael",
            "name_4_l": "smith",
            "name_4_r": "smith",
        },
        {
            "name_1_l": "jonathon",
            "name_1_r": "richard",
            "name_2_l": "richard",
            "name_2_r": "jonathan",
            "name_3_l": "michael",
            "name_3_r": "michael",
            "name_4_l": "smith",
            "name_4_r": "smith",
        },
        {
            "name_1_l": "caitlin",
            "name_1_r": "michael",
            "name_2_l": "richard",
            "name_2_r": "richard",
            "name_3_l": "michael",
            "name_3_r": "smith",
            "name_4_l": "smith",
            "name_4_r": "katelyn",
        },
        {
            "name_1_l": "john",
            "name_1_r": "james",
            "name_2_l": "richard",
            "name_2_r": "richard",
            "name_3_l": "michael",
            "name_3_r": "michael",
            "name_4_l": "smith",
            "name_4_r": "smith",
        },
        {
            "name_1_l": None,
            "name_1_r": "james",
            "name_2_l": "richard",
            "name_2_r": "richard",
            "name_3_l": "michael",
            "name_3_r": "michael",
            "name_4_l": "smith",
            "name_4_r": "smith",
        },
        {
            "name_1_l": "richard",
            "name_1_r": "john",
            "name_2_l": "john",
            "name_2_r": "richard",
            "name_3_l": "michael",
            "name_3_r": None,
            "name_4_l": "smith",
            "name_4_r": None,
        },
    ]

    df = spark.createDataFrame(Row(**x) for x in data_list)
    df.createOrReplaceTempView("df")

    sql = f"""
    select
    {sql_gen_gammas_name_inversion_4("name_1",["name_2","name_3","name_4"])} as result_1
    from df
    """

    df_pd = spark.sql(sql).toPandas()

    result = list(df_pd["result_1"])
    expected = [3, 2, 2, 0, 0, -1, 2]
    assert result == expected

    result = list(df_pd["result_1"])
    expected = [3, 2, 2, 0, 0, -1, 2]
    assert result == expected
