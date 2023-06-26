import pandas as pd

from splink.duckdb.linker import DuckDBLinker
from splink.spark.linker import SparkLinker

full_name_cc = {
    "output_column_name": "full_name",
    "comparison_levels": [
        {
            "sql_condition": """full_name_l IS NULL OR full_name_r IS NULL
            or length(full_name_l) < 2 or length(full_name_r) < 2""",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "full_name_l = full_name_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.7,
            "u_probability": 0.1,
            "tf_adjustment_column": "full_name",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "levenshtein(full_name_l, full_name_r) <= 2",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "levenshtein <= 2",
        },
        {
            "sql_condition": "levenshtein(full_name_l, full_name_r) <= 4",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "levenshtein <= 4",
        },
        {
            "sql_condition": "levenshtein(full_name_l, full_name_r) <= 8",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "levenshtein <= 8",
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.8,
        },
    ],
}


dob_cc = {
    "output_column_name": "dob",
    "comparison_levels": [
        {
            "sql_condition": "dob_l IS NULL OR dob_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "dob_l = dob_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

birth_place_cc = {
    "output_column_name": "birth_place",
    "comparison_levels": [
        {
            "sql_condition": "birth_place_l IS NULL OR birth_place_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "birth_place_l = birth_place_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

postcode_cc = {
    "output_column_name": "postcode",
    "comparison_levels": [
        {
            "sql_condition": "postcode_l IS NULL OR postcode_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "postcode_l = postcode_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
            "tf_adjustment_column": "postcode",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}


occupation_cc = {
    "output_column_name": "occupation",
    "comparison_levels": [
        {
            "sql_condition": "occupation_l IS NULL OR occupation_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "occupation_l = occupation_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
            "tf_adjustment_column": "occupation",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

settings_dict = {
    "probability_two_random_records_match": 0.01,
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.postcode = r.postcode and substr(l.full_name,1,2) = substr(r.full_name,1,2)",
        "l.dob = r.dob and substr(l.postcode,1,2) = substr(r.postcode,1,2)",
        "l.postcode = r.postcode and substr(l.dob,1,3) = substr(r.dob,1,3)",
        "l.postcode = r.postcode and substr(l.dob,4,5) = substr(r.dob,4,5)",
    ],
    "comparisons": [
        full_name_cc,
        dob_cc,
        birth_place_cc,
        postcode_cc,
        occupation_cc,
    ],
    "retain_matching_columns": False,
    "retain_intermediate_calculation_columns": False,
    "additional_columns_to_retain": ["cluster"],
    "max_iterations": 10,
}


def duckdb_performance(df, max_pairs):
    linker = DuckDBLinker(df, settings_dict)

    linker.estimate_u_using_random_sampling(max_pairs=max_pairs)

    linker.estimate_parameters_using_expectation_maximisation(
        "l.full_name = r.full_name"
    )

    linker.estimate_parameters_using_expectation_maximisation(
        "l.dob = r.dob and substr(l.postcode,1,2) = substr(r.postcode,1,2)"
    )

    df = linker.predict()
    df.as_pandas_dataframe()


def test_3_rounds_100k_duckdb(benchmark):
    df = pd.read_parquet("./benchmarking/synthetic_data_all.parquet")
    df = df.head(100_000)

    benchmark.pedantic(
        duckdb_performance,
        kwargs={"df": df, "max_pairs": 2e6},
        rounds=3,
        iterations=1,
        warmup_rounds=0,
    )


def test_3_rounds_300k_duckdb(benchmark):
    df = pd.read_parquet("./benchmarking/synthetic_data_all.parquet")
    df = df.head(300_000)

    benchmark.pedantic(
        duckdb_performance,
        kwargs={"df": df, "max_pairs": 2e6},
        rounds=3,
        iterations=1,
        warmup_rounds=0,
    )


def test_3_rounds_1m_duckdb(benchmark):
    df = pd.read_parquet("./benchmarking/synthetic_data_all.parquet")

    benchmark.pedantic(
        duckdb_performance,
        kwargs={"df": df, "max_pairs": 10e6},
        rounds=3,
        iterations=1,
        warmup_rounds=0,
    )


def spark_performance(df, max_pairs=1e6):
    linker = SparkLinker(
        df,
        settings_dict,
    )

    linker.estimate_u_using_random_sampling(max_pairs=max_pairs)

    linker.estimate_parameters_using_expectation_maximisation(
        "l.full_name = r.full_name"
    )

    linker.estimate_parameters_using_expectation_maximisation(
        "l.dob = r.dob and substr(l.postcode,1,2) = substr(r.postcode,1,2)"
    )

    df = linker.predict()
    df.as_pandas_dataframe()


def test_3_rounds_100k_spark(benchmark):
    from pyspark.context import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    def setup():
        conf = SparkConf()
        conf.set("spark.driver.memory", "12g")
        conf.set("spark.sql.shuffle.partitions", "8")
        conf.set("spark.default.parallelism", "8")

        sc = SparkContext.getOrCreate(conf=conf)
        sc.setCheckpointDir("./tmp/splink_checkpoints")
        spark = SparkSession(sc)

        for table in spark.catalog.listTables():
            if table.isTemporary:
                spark.catalog.dropTempView(table.name)

        df = spark.read.parquet("./benchmarking/synthetic_data_all.parquet")
        df = df.limit(100_000)

        return (df,), {"max_pairs": 2e6}

    benchmark.pedantic(
        spark_performance,
        setup=setup,
        rounds=3,
        iterations=1,
        warmup_rounds=0,
    )


def test_3_rounds_300k_spark(benchmark):
    from pyspark.context import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    def setup():
        conf = SparkConf()
        conf.set("spark.driver.memory", "12g")
        conf.set("spark.sql.shuffle.partitions", "8")
        conf.set("spark.default.parallelism", "8")

        sc = SparkContext.getOrCreate(conf=conf)
        spark = SparkSession(sc)

        for table in spark.catalog.listTables():
            if table.isTemporary:
                spark.catalog.dropTempView(table.name)

        df = spark.read.parquet("./benchmarking/synthetic_data_all.parquet")
        df = df.limit(300_000)

        return (df,), {"max_pairs": 2e6}

    benchmark.pedantic(
        spark_performance,
        setup=setup,
        rounds=3,
        iterations=1,
        warmup_rounds=0,
    )


def test_3_rounds_1m_spark(benchmark):
    from pyspark.context import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    def setup():
        conf = SparkConf()
        conf.set("spark.driver.memory", "12g")
        conf.set("spark.sql.shuffle.partitions", "8")
        conf.set("spark.default.parallelism", "8")

        sc = SparkContext.getOrCreate(conf=conf)
        sc.setCheckpointDir("./tmp/splink_checkpoints")
        spark = SparkSession(sc)

        for table in spark.catalog.listTables():
            if table.isTemporary:
                spark.catalog.dropTempView(table.name)

        df = spark.read.parquet("./benchmarking/synthetic_data_all.parquet")

        return (df,), {"max_pairs": 10e6}

    benchmark.pedantic(
        spark_performance,
        setup=setup,
        rounds=3,
        iterations=1,
        warmup_rounds=0,
    )
