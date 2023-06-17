import pandas as pd

import splink.spark.comparison_level_library as cll
from splink.spark.linker import SparkLinker

first_name_cc = {
    "output_column_name": "first_name",
    "comparison_levels": [
        cll.null_level("first_name"),
        {
            "sql_condition": "Dmetaphone(first_name_l) = Dmetaphone(first_name_r)",
            "label_for_charts": "demeta",
        },
        {
            "sql_condition": "jaro_winkler(first_name_l, first_name_r) >= 0.95",
            "label_for_charts": "jaro_winkler >= 0.95",
        },
        cll.else_level(),
    ],
}

surname_cc = {
    "output_column_name": "surname",
    "comparison_levels": [
        cll.null_level("surname"),
        {
            "sql_condition": "DmetaphoneAlt(surname_l) = DmetaphoneAlt(surname_r)",
            "label_for_charts": "demeta_alt",
        },
        {
            "sql_condition": "cosine_distance(surname_l, surname_r) <= 0.95",
            "label_for_charts": "cosine_distance <= 0.95",
        },
        cll.else_level(),
    ],
}


settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.surname = r.surname",
    ],
    "comparisons": [
        first_name_cc,
        surname_cc,
    ],
    "retain_matching_columns": False,
    "retain_intermediate_calculation_columns": False,
    "max_iterations": 1,
}


def test_udf_registration(spark):
    # Integration test to ensure spark loads our udfs without any issues
    df_spark = spark.read.csv(
        "tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )

    linker = SparkLinker(
        df_spark,
        settings,
    )
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    blocking_rule = "l.first_name = r.first_name"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)
    blocking_rule = "l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    linker.predict()


def test_damerau_levenshtein(spark):
    data = ["dave", "david", "", "dave"]
    df = pd.DataFrame(data, columns=["test_names"])
    df["id"] = df.index
    df_spark_dam_lev = spark.createDataFrame(df)

    linker = SparkLinker(
        df_spark_dam_lev,
        settings,
        input_table_aliases="test_dl_df",
    )

    sql = """
        select

        /* Output test names for easier review */
        l.test_names as test_names_l, r.test_names as test_names_r,

        /* Calculate damerau-levenshtein results for our test cases */
        damerau_levenshtein(l.test_names, r.test_names) as dl_test
        from test_dl_df as l

        inner join
        test_dl_df as r

        where l.id < r.id
    """

    udf_out = linker.query_sql(sql)
    # Set accuracy level
    decimals = 4

    # Test damerau-levenshtein outputs are correct
    dl_w_out = tuple(udf_out.dl_test.round(decimals=decimals))
    dl_expected = (2.0, 4.0, 0.0, 5.0, 2.0, 4.0)

    assert dl_w_out == dl_expected

    # ensure that newest jar is calculating similarity . dl of strings below is 0.9440
    assert (
        spark.sql("""SELECT damerau_levenshtein("MARHTA", "MARTHA")  """).first()[0]
        == 1.0
    )

    # ensure totally dissimilar strings have dl sim of 5
    assert (
        spark.sql("""SELECT damerau_levenshtein("Local", "Pub")  """).first()[0] == 5.0
    )

    # ensure totally similar strings have dl sim of 0
    assert spark.sql("""SELECT damerau_levenshtein("Pub", "Pub")  """).first()[0] == 0.0

    # testcases taken from jaro article on jw sim
    assert (
        round(
            spark.sql("""SELECT damerau_levenshtein("hello", "hallo")  """).first()[0],
            decimals,
        )
        == 1.0
    )

    assert (
        round(
            spark.sql("""SELECT damerau_levenshtein("hippo", "elephant")  """).first()[
                0
            ],
            decimals,
        )
        == 7.0
    )
    assert (
        round(
            spark.sql("""SELECT damerau_levenshtein("elephant", "hippo")  """).first()[
                0
            ],
            decimals,
        )
        == 7.0
    )
    assert (
        spark.sql("""SELECT damerau_levenshtein("aaapppp", "")  """).first()[0] == 7.0
    )


def test_jaro(spark):
    data = ["dave", "david", "", "dave"]
    df = pd.DataFrame(data, columns=["test_names"])
    df["id"] = df.index
    df_spark_jaro = spark.createDataFrame(df)

    linker = SparkLinker(
        df_spark_jaro,
        settings,
        input_table_aliases="test_jaro_df",
    )

    sql = """
        select

        /* Output test names for easier review */
        l.test_names as test_names_l, r.test_names as test_names_r,

        /* Calculate jaro results for our test cases */
        jaro_sim(l.test_names, r.test_names) as jaro_test
        from test_jaro_df as l

        inner join
        test_jaro_df as r

        where l.id < r.id
    """

    udf_out = linker.query_sql(sql)
    # Set accuracy level
    decimals = 4

    # Test jaro-winkler outputs are correct
    jaro_w_out = tuple(udf_out.jaro_test.round(decimals=decimals))
    jaro_expected = (0.7833, 0.0, 1.0, 0.0, 0.7833, 0.0)

    assert jaro_w_out == jaro_expected

    # ensure that newest jar is calculating similarity . jw of strings below is 0.9440
    assert spark.sql('SELECT jaro_sim("MARHTA", "MARTHA")').first()[0] > 0.9

    # ensure that when one or both of the strings compared is NULL jw sim is 0

    assert spark.sql("""SELECT jaro_sim(NULL, "John")  """).first()[0] == 0.0
    assert spark.sql("""SELECT jaro_sim("Tom", NULL )  """).first()[0] == 0.0
    assert spark.sql("""SELECT jaro_sim(NULL, NULL )  """).first()[0] == 0.0

    # ensure totally dissimilar strings have jw sim of 0
    assert spark.sql("""SELECT jaro_sim("Local", "Pub")  """).first()[0] == 0.0

    # ensure totally similar strings have jw sim of 1
    assert spark.sql("""SELECT jaro_sim("Pub", "Pub")  """).first()[0] == 1.0

    # testcases taken from jaro article on jw sim
    assert (
        round(
            spark.sql("""SELECT jaro_sim("hello", "hallo")  """).first()[0],
            decimals,
        )
        == 0.8667
    )

    assert (
        round(
            spark.sql("""SELECT jaro_sim("hippo", "elephant")  """).first()[0],
            decimals,
        )
        == 0.4417
    )
    assert (
        round(
            spark.sql("""SELECT jaro_sim("elephant", "hippo")  """).first()[0],
            decimals,
        )
        == 0.4417
    )
    assert spark.sql("""SELECT jaro_sim("aaapppp", "")  """).first()[0] == 0.0


def test_jaro_winkler(spark):
    data = ["dave", "david", "", "dave"]
    df = pd.DataFrame(data, columns=["test_names"])
    df["id"] = df.index
    df_spark_jaro_winkler = spark.createDataFrame(df)

    linker = SparkLinker(
        df_spark_jaro_winkler,
        settings,
        input_table_aliases="test_jw_df",
    )

    sql = """
        select

        /* Output test names for easier review */
        l.test_names as test_names_l, r.test_names as test_names_r,

        /* Calculate jaro-winkler results for our test cases */
        jaro_winkler(l.test_names, r.test_names) as jaro_winkler_test
        from test_jw_df as l

        inner join
        test_jw_df as r

        where l.id < r.id
    """

    udf_out = linker.query_sql(sql)
    # Set accuracy level
    decimals = 4

    # Test jaro-winkler outputs are correct
    jaro_w_out = tuple(udf_out.jaro_winkler_test.round(decimals=decimals))
    jaro_expected = (0.8483, 0.0, 1.0, 0.0, 0.8483, 0.0)

    assert jaro_w_out == jaro_expected

    # ensure that newest jar is calculating similarity . jw of strings below is 0.961111
    assert spark.sql("""SELECT jaro_winkler("MARHTA", "MARTHA")  """).first()[0] > 0.9

    # ensure that when one or both of the strings compared is NULL jw sim is 0

    assert spark.sql("""SELECT jaro_winkler(NULL, "John")  """).first()[0] == 0.0
    assert spark.sql("""SELECT jaro_winkler("Tom", NULL )  """).first()[0] == 0.0
    assert spark.sql("""SELECT jaro_winkler(NULL, NULL )  """).first()[0] == 0.0

    # ensure totally dissimilar strings have jw sim of 0
    assert spark.sql("""SELECT jaro_winkler("Local", "Pub")  """).first()[0] == 0.0

    # ensure totally similar strings have jw sim of 1
    assert spark.sql("""SELECT jaro_winkler("Pub", "Pub")  """).first()[0] == 1.0

    # testcases taken from jaro winkler article on jw sim
    assert spark.sql("""SELECT jaro_winkler("hello", "hallo")  """).first()[0] == 0.88

    assert (
        round(
            spark.sql("""SELECT jaro_winkler("hippo", "elephant")  """).first()[0],
            decimals,
        )
        == 0.4417
    )
    assert (
        round(
            spark.sql("""SELECT jaro_winkler("elephant", "hippo")  """).first()[0],
            decimals,
        )
        == 0.4417
    )
    assert spark.sql("""SELECT jaro_winkler("aaapppp", "")  """).first()[0] == 0.0
