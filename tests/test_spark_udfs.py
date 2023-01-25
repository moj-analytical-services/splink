import splink.spark.spark_comparison_level_library as cll
from splink.spark.spark_linker import SparkLinker
import pandas as pd

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


def setup_spark():
    from splink.spark.jar_location import similarity_jar_location
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession

    conf = SparkConf()
    # Add custom similarity functions, which are bundled with Splink
    # documented here: https://github.com/moj-analytical-services/splink_scalaudfs
    path = similarity_jar_location()
    conf.set("spark.jars", path)
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)
    spark.sparkContext.setCheckpointDir("./tmp_checkpoints")

    return spark


def test_udf_registration():
    # Integration test to ensure spark loads our udfs without any issues
    spark = setup_spark()
    df_spark = spark.read.csv(
        "tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )

    linker = SparkLinker(
        df_spark,
        settings,
    )
    linker.estimate_u_using_random_sampling(target_rows=1e6)
    blocking_rule = "l.first_name = r.first_name"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)
    blocking_rule = "l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    linker.predict()


def test_udf_functionality():
    spark = setup_spark()
    data = ["dave", "david", "", "dave"]
    df = pd.DataFrame(data, columns=["test_names"])
    df["id"] = df.index
    df_spark = spark.createDataFrame(df)

    linker = SparkLinker(
        df_spark,
        settings,
        input_table_aliases="test_df",
    )

    sql = """
        select

        /* Output test names for easier review */
        l.test_names as test_names_l, r.test_names as test_names_r,

        /* Calculate jaro-winkler results for our test cases */
        jaro_winkler(l.test_names, r.test_names) as jaro_winkler_test
        from test_df as l

        inner join
        test_df as r

        where l.id < r.id
    """

    udf_out = linker.query_sql(sql)
    # Set accuracy level
    decimals = 6

    # Test jaro-winkler outputs are correct
    jaro_w_out = tuple(udf_out.jaro_winkler_test.round(decimals=decimals))
    jaro_expected = (0.848333, 0.0, 1.0, 0.0, 0.848333, 0.0)

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
        spark.sql("""SELECT jaro_winkler("hippo", "elephant")  """).first()[0] < 0.45
    )  # its 0.44166666666666665
    assert (
        spark.sql("""SELECT jaro_winkler("elephant", "hippo")  """).first()[0] < 0.45
    )  # its 0.44166666666666665
    assert spark.sql("""SELECT jaro_winkler("aaapppp", "")  """).first()[0] == 0.0
