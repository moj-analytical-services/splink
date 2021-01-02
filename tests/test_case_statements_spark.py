from pyspark.sql import Row

from splink.case_statements import (
    sql_gen_case_stmt_array_intersect_2,
    sql_gen_case_stmt_array_intersect_3,
    sql_gen_case_stmt_array_combinations_leven_abs_3,
    sql_gen_case_stmt_array_combinations_leven_rel_3,
    sql_gen_case_stmt_case_stmt_jaro_2,
    sql_gen_case_stmt_case_stmt_jaro_3,
    sql_gen_case_stmt_case_stmt_jaro_4,
    sql_gen_case_stmt_array_combinations_jaro_3,
    sql_gen_case_stmt_array_combinations_jaro_dmeta_4,
    sql_gen_case_stmt_levenshtein_rel_3,
    sql_gen_case_stmt_levenshtein_rel_4,
    sql_gen_case_stmt_name_inversion_4,
    _check_jaro_registered,
    sql_gen_case_smnt_strict_equality_2,
    sql_gen_case_stmt_numeric_abs_3,
    sql_gen_case_stmt_numeric_abs_4,
    sql_gen_case_stmt_numeric_perc_3,
    sql_gen_case_stmt_numeric_perc_4,
)
import pytest


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
    {sql_gen_case_stmt_array_combinations_leven_abs_3("arr_comp")} as result_l3_1,
    {sql_gen_case_stmt_array_combinations_leven_abs_3("arr_comp", zero_length_is_null=False)} as result_l3_2,
    {sql_gen_case_stmt_array_combinations_jaro_3("arr_comp")} as result_j3,
    {sql_gen_case_stmt_array_combinations_jaro_dmeta_4("arr_comp")} as result_jd4
    from df
    """

    df_pd = spark.sql(sql).toPandas()
    assert list(df_pd["result_ai2_1"]) == [1, 1, 0, 0, -1, -1]
    assert list(df_pd["result_ai2_2"]) == [1, 1, 0, 0, 0, -1]
    assert list(df_pd["result_ai3"]) == [2, 1, 0, 0, -1, -1]
    assert list(df_pd["result_l3_1"]) == [2, 2, 2, 1, -1, -1]
    assert list(df_pd["result_l3_2"]) == [2, 2, 2, 1, 0, -1]
    assert list(df_pd["result_j3"]) == [2, 2, 1, 1, -1, -1]
    assert list(df_pd["result_jd4"]) == [3, 3, 2, 1, -1, -1]


def test_leven_rel(spark):

    data_list = [
        {
            "arr_comp_l": ["0123456789", "abcdefghij"],
            "arr_comp_r": ["0123456789", "abcdefghij"],
        },
        {
            "arr_comp_l": ["0123456789", "abcdefghij"],
            "arr_comp_r": ["0123456789", "qrstuvwxyz"],
        },
        {
            "arr_comp_l": ["012345678", "abcdefghij"],
            "arr_comp_r": ["0123456789", "qrstuvwxyz"],
        },
        {
            "arr_comp_l": ["01234567", "abcdefghij"],
            "arr_comp_r": ["0123456789", "qrstuvwxyz"],
        },
        {
            "arr_comp_l": ["01234", "abcdefghij"],
            "arr_comp_r": ["0123456789", "qrstuvwxyz"],
        },
        {"arr_comp_l": ["0123456789", "abcdefghij"], "arr_comp_r": []},
        {"arr_comp_l": ["0123456789", "abcdefghij"], "arr_comp_r": None},
    ]
    from splink.case_statements import _compare_pairwise_combinations_leven_prop

    df = spark.createDataFrame(Row(**x) for x in data_list)
    df.createOrReplaceTempView("df")

    sql = f"""
    select
    {sql_gen_case_stmt_array_combinations_leven_rel_3("arr_comp", threshold1=0.15, threshold2=0.3)} as result
    from df
    """

    df = spark.sql(sql)
    df_pd = df.toPandas()

    assert list(df_pd["result"]) == [2, 2, 2, 1, 0, -1, -1]


def test_name_inversion(spark):

    data_list = [
        {
            "name_1": ("john", "john"),
            "name_2": ("richard", "richard"),
            "name_3": ("michael", "michael"),
            "name_4": ("smith", "smith"),
        },
        {
            "name_1": ("richard", "john"),
            "name_2": ("john", "richard"),
            "name_3": ("michael", "michael"),
            "name_4": ("smith", "smith"),
        },
        {
            "name_1": ("jonathon", "richard"),
            "name_2": ("richard", "jonathan"),
            "name_3": ("michael", "michael"),
            "name_4": ("smith", "smith"),
        },
        {
            "name_1": ("caitlin", "michael"),
            "name_2": ("richard", "richard"),
            "name_3": ("michael", "smith"),
            "name_4": ("smith", "katelyn"),
        },
        {
            "name_1": ("john", "james"),
            "name_2": ("richard", "richard"),
            "name_3": ("michael", "michael"),
            "name_4": ("smith", "smith"),
        },
        {
            "name_1": (None, "james"),
            "name_2": ("richard", "richard"),
            "name_3": ("michael", "michael"),
            "name_4": ("smith", "smith"),
        },
        {
            "name_1": ("richard", "john"),
            "name_2": ("john", "richard"),
            "name_3": ("michael", None),
            "name_4": ("smith", None),
        },
    ]
    rows = []
    for row in data_list:
        comparison_row = {}
        for key in row.keys():
            comparison_row[f"{key}_l"] = row[key][0]
            comparison_row[f"{key}_r"] = row[key][1]
        rows.append(comparison_row)

    df = spark.createDataFrame(Row(**x) for x in rows)
    df.createOrReplaceTempView("df")

    sql = f"""
    select
    {sql_gen_case_stmt_name_inversion_4("name_1",["name_2","name_3","name_4"])} as result_1
    from df
    """

    df_pd = spark.sql(sql).toPandas()

    result = list(df_pd["result_1"])
    expected = [3, 2, 2, 0, 0, -1, 2]
    assert result == expected

    result = list(df_pd["result_1"])
    expected = [3, 2, 2, 0, 0, -1, 2]
    assert result == expected


def test_jaro_warning(spark):
    assert _check_jaro_registered(spark) == True

    spark.sql("drop temporary function jaro_winkler_sim")
    with pytest.warns(UserWarning):
        assert _check_jaro_registered(spark) == False
    from pyspark.sql.types import DoubleType

    spark.udf.registerJavaFunction(
        "jaro_winkler_sim",
        "uk.gov.moj.dash.linkage.JaroWinklerSimilarity",
        DoubleType(),
    )


@pytest.fixture(scope="module")
def str_comp_data(spark):

    rows = [
        {
            "str_col_l": "these strings are equal",
            "str_col_r": "these strings are equal",
        },
        {
            "str_col_l": "these strings are almost equal",
            "str_col_r": "these strings are almos equal",
        },
        {
            "str_col_l": "these strings are almost equal",
            "str_col_r": "not the same at all",
        },
        {"str_col_l": "these strings are almost equal", "str_col_r": None},
        {"str_col_l": None, "str_col_r": None},
    ]
    df = spark.createDataFrame(Row(**x) for x in rows)
    df.createOrReplaceTempView("str_comp")
    return df


def test_leven(spark, str_comp_data):
    case_strict = sql_gen_case_smnt_strict_equality_2("str_col", "strict")
    case_l3 = sql_gen_case_stmt_levenshtein_rel_3("str_col", "leven_3")
    case_l4 = sql_gen_case_stmt_levenshtein_rel_4("str_col", "leven_4")
    sql = f"""select
                {case_strict},
                {case_l3},
                {case_l4}
                from str_comp"""
    df = spark.sql(sql).toPandas()

    assert list(df["gamma_strict"]) == [1, 0, 0, -1, -1]
    assert list(df["gamma_leven_3"]) == [2, 1, 0, -1, -1]
    assert list(df["gamma_leven_4"]) == [3, 2, 0, -1, -1]


def test_jaro(spark, str_comp_data):

    jaro_2 = sql_gen_case_stmt_case_stmt_jaro_2("str_col", "jaro_2")
    jaro_3 = sql_gen_case_stmt_case_stmt_jaro_3("str_col", "jaro_3")
    jaro_4 = sql_gen_case_stmt_case_stmt_jaro_4("str_col", "jaro_4", threshold3=0.001)

    sql = f"""select
    {jaro_2},
    {jaro_3},
    {jaro_4}
    from str_comp"""
    df = spark.sql(sql).toPandas()

    assert list(df["gamma_jaro_2"]) == [1, 1, 0, -1, -1]
    assert list(df["gamma_jaro_3"]) == [2, 2, 0, -1, -1]
    assert list(df["gamma_jaro_4"]) == [3, 3, 1, -1, -1]


def test_numeric(spark):

    rows = [
        {"float_col_l": 1.0, "float_col_r": 1.0},
        {"float_col_l": 100.0, "float_col_r": 99.9},
        {"float_col_l": 100.0, "float_col_r": 90.1},
        {"float_col_l": -100.0, "float_col_r": -85.1},
        {"float_col_l": None, "float_col_r": -85.1},
    ]

    df = spark.createDataFrame(Row(**x) for x in rows)
    df.createOrReplaceTempView("float_comp")

    numeric_abs_3 = sql_gen_case_stmt_numeric_abs_3(
        "float_col", gamma_col_name="numeric_abs_3", abs_amount=1
    )
    numeric_abs_4 = sql_gen_case_stmt_numeric_abs_4(
        "float_col",
        abs_amount_low=1,
        abs_amount_high=10,
        gamma_col_name="numeric_abs_4",
    )
    numeric_perc_3a = sql_gen_case_stmt_numeric_perc_3(
        "float_col", per_diff=0.01, gamma_col_name="numeric_perc_3a"
    )
    numeric_perc_3b = sql_gen_case_stmt_numeric_perc_3(
        "float_col", per_diff=0.20, gamma_col_name="numeric_perc_3b"
    )

    numeric_perc_4 = sql_gen_case_stmt_numeric_perc_4(
        "float_col",
        per_diff_low=0.01,
        per_diff_high=0.1,
        gamma_col_name="numeric_perc_4",
    )

    sql = f"""select
    {numeric_abs_3},
    {numeric_abs_4},
    {numeric_perc_3a},
    {numeric_perc_3b},
    {numeric_perc_4}
    from float_comp"""
    df = spark.sql(sql).toPandas()

    assert list(df["gamma_numeric_abs_3"]) == [2, 1, 0, 0, -1]
    assert list(df["gamma_numeric_abs_4"]) == [3, 2, 1, 0, -1]
    assert list(df["gamma_numeric_perc_3a"]) == [2, 1, 0, 0, -1]
    assert list(df["gamma_numeric_perc_3b"]) == [2, 1, 1, 1, -1]
    assert list(df["gamma_numeric_perc_4"]) == [3, 2, 1, 0, -1]
