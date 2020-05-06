# Derivations for a lot of the expected answers here:
# https://github.com/moj-analytical-services/splink/blob/dev/tests/expectation_maximisation_test_answers.xlsx
import copy
import os

from splink import Splink, load_from_json
from splink.blocking import block_using_rules
from splink.params import Params
from splink.gammas import add_gammas, complete_settings_dict
from splink.iterate import iterate
from splink.expectation_step import run_expectation_step, get_overall_log_likelihood
from splink.case_statements import *
from splink.case_statements import _check_jaro_registered
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import logging

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def spark():

    try:
        import pyspark
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SparkSession
        from pyspark.sql import types

        conf = SparkConf()

        conf.set("spark.sql.shuffle.partitions", "1")
        conf.set("spark.jars.ivy", "/home/jovyan/.ivy2/")
        conf.set("spark.driver.extraClassPath", "jars/scala-udf-similarity-0.0.6.jar")
        conf.set("spark.jars", "jars/scala-udf-similarity-0.0.6.jar")
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.sql.shuffle.partitions", "24")

        sc = SparkContext.getOrCreate(conf=conf)

        spark = SparkSession(sc)

        udfs = [
            ("jaro_winkler_sim", "JaroWinklerSimilarity", types.DoubleType()),
            ("jaccard_sim", "JaccardSimilarity", types.DoubleType()),
            ("cosine_distance", "CosineDistance", types.DoubleType()),
            ("Dmetaphone", "DoubleMetaphone", types.StringType()),
            ("QgramTokeniser", "QgramTokeniser", types.StringType()),
            ("Q3gramTokeniser", "Q3gramTokeniser", types.StringType()),
            ("Q4gramTokeniser", "Q4gramTokeniser", types.StringType()),
            ("Q5gramTokeniser", "Q5gramTokeniser", types.StringType()),
        ]

        for a, b, c in udfs:
            spark.udf.registerJavaFunction(a, "uk.gov.moj.dash.linkage." + b, c)
        SPARK_EXISTS = True
    except:
        SPARK_EXISTS = False

    if SPARK_EXISTS:
        print("Spark exists, running spark tests")
        yield spark
    else:
        spark = None
        logger.error("Spark not available")
        print("Spark not available")
        yield spark


from pyspark.sql import SparkSession, Row


def test_no_blocking(spark, link_dedupe_data):
    settings = {
        "link_type": "link_only",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}],
        "blocking_rules": []
    }
    settings = complete_settings_dict(settings, spark=None)
    dfpd_l = pd.read_sql("select * from df_l", link_dedupe_data)
    dfpd_r = pd.read_sql("select * from df_r", link_dedupe_data)
    df_l = spark.createDataFrame(dfpd_l)
    df_r = spark.createDataFrame(dfpd_r)


    df_comparison = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df_comparison.toPandas()
    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1,1,1,2,2,2]
    assert list(df["unique_id_r"]) == [7,8,9,7,8,9]

def test_expectation(spark, sqlite_con_1, params_1, gamma_settings_1):
    dfpd = pd.read_sql("select * from test1", sqlite_con_1)
    df = spark.createDataFrame(dfpd)

    gamma_settings_1["blocking_rules"] = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    df_comparison = block_using_rules(gamma_settings_1, df=df, spark=spark)

    df_gammas = add_gammas(df_comparison, gamma_settings_1, spark)

    # df_e = iterate(df_gammas, spark, params_1, num_iterations=1)
    df_e = run_expectation_step(df_gammas, params_1, gamma_settings_1, spark)

    df_e_pd = df_e.toPandas()
    df_e_pd = df_e_pd.sort_values(["unique_id_l", "unique_id_r"])

    correct_list = [
        0.893617021,
        0.705882353,
        0.705882353,
        0.189189189,
        0.189189189,
        0.893617021,
        0.375,
        0.375,
    ]
    result_list = list(df_e_pd["match_probability"].astype(float))

    for i in zip(result_list, correct_list):
        assert i[0] == pytest.approx(i[1])


def test_tiny_numbers(spark, sqlite_con_1):

    # Regression test, see https://github.com/moj-analytical-services/splink/issues/48

    dfpd = pd.read_sql("select * from test1", sqlite_con_1)
    df = spark.createDataFrame(dfpd)

    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.4,
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "m_probabilities": [5.9380419956766985e-25, 1 - 5.9380419956766985e-25],
                "u_probabilities": [0.8, 0.2],
            },
            {"col_name": "surname", "num_levels": 2,},
        ],
        "blocking_rules": ["l.mob = r.mob", "l.surname = r.surname",],
    }

    settings = complete_settings_dict(settings, spark=None)

    df_comparison = block_using_rules(settings, df=df, spark=spark)

    df_gammas = add_gammas(df_comparison, settings, spark)
    params = Params(settings, spark="supress_warnings")

    df_e = run_expectation_step(df_gammas, params, settings, spark)


def test_iterate(spark, sqlite_con_1, params_1, gamma_settings_1):

    original_params = copy.deepcopy(params_1.params)
    dfpd = pd.read_sql("select * from test1", sqlite_con_1)
    df = spark.createDataFrame(dfpd)

    rules = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    gamma_settings_1["blocking_rules"] = rules

    df_comparison = block_using_rules(gamma_settings_1, df=df, spark=spark)

    df_gammas = add_gammas(df_comparison, gamma_settings_1, spark)

    gamma_settings_1["max_iterations"] = 1
    df_e = iterate(df_gammas, params_1, gamma_settings_1, spark)

    assert params_1.params["λ"] == pytest.approx(0.540922141)

    assert params_1.params["π"]["gamma_mob"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.087438272, abs=0.0001)
    assert params_1.params["π"]["gamma_surname"]["prob_dist_non_match"]["level_1"][
        "probability"
    ] == pytest.approx(0.160167628, abs=0.0001)

    first_it_params = copy.deepcopy(params_1.params)

    df_e_pd = df_e.toPandas()
    df_e_pd = df_e_pd.sort_values(["unique_id_l", "unique_id_r"])

    correct_list = [
        0.658602114,
        0.796821727,
        0.796821727,
        0.189486495,
        0.189486495,
        0.658602114,
        0.495063367,
        0.495063367,
    ]
    result_list = list(df_e_pd["match_probability"].astype(float))

    for i in zip(result_list, correct_list):
        assert i[0] == pytest.approx(i[1], abs=0.0001)

    # Does it still work with another iteration?
    gamma_settings_1["max_iterations"] = 1
    df_e = iterate(df_gammas, params_1, gamma_settings_1, spark)
    assert params_1.params["λ"] == pytest.approx(0.534993426, abs=0.0001)

    assert params_1.params["π"]["gamma_mob"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.088546179, abs=0.0001)
    assert params_1.params["π"]["gamma_surname"]["prob_dist_non_match"]["level_1"][
        "probability"
    ] == pytest.approx(0.109234086, abs=0.0001)

    ## Test whether the params object is correctly storing the iteration history

    assert params_1.param_history[0] == original_params
    assert params_1.param_history[1] == first_it_params

    ## Now test whether, when we

    data = params_1._convert_params_dict_to_dataframe(original_params)
    val1 = {
        "gamma": "gamma_mob",
        "match": 0,
        "value_of_gamma": "level_0",
        "probability": 0.8,
        "value": 0,
        "column": "mob",
    }
    val2 = {
        "gamma": "gamma_surname",
        "match": 1,
        "value_of_gamma": "level_1",
        "probability": 0.2,
        "value": 1,
        "column": "surname",
    }

    assert val1 in data
    assert val2 in data

    correct_list = [{"iteration": 0, "λ": 0.4}, {"iteration": 1, "λ": 0.540922141}]

    result_list = params_1._iteration_history_df_lambdas()

    for i in zip(result_list, correct_list):
        assert i[0]["iteration"] == i[1]["iteration"]
        assert i[0]["λ"] == pytest.approx(i[1]["λ"])

    result_list = params_1._iteration_history_df_gammas()

    val1 = {
        "iteration": 0,
        "gamma": "gamma_mob",
        "match": 0,
        "value_of_gamma": "level_0",
        "probability": 0.8,
        "value": 0,
        "column": "mob",
    }
    assert val1 in result_list

    val2 = {
        "iteration": 1,
        "gamma": "gamma_surname",
        "match": 0,
        "value_of_gamma": "level_1",
        "probability": 0.160167628,
        "value": 1,
        "column": "surname",
    }

    for r in result_list:
        if r["iteration"] == 1:
            if r["gamma"] == "gamma_surname":
                if r["match"] == 0:
                    if r["value"] == 1:
                        record = r

    for k, v in record.items():
        expected_value = val2[k]
        if k == "probability":
            assert v == pytest.approx(expected_value, abs=0.0001)
        else:
            assert v == expected_value

    # Test whether saving and loading parameters works
    import tempfile

    dir = tempfile.TemporaryDirectory()
    fname = os.path.join(dir.name, "params.json")

    # print(params_1.params)
    # import json
    # print(json.dumps(params_1.to_dict(), indent=4))

    params_1.save_params_to_json_file(fname)

    from splink.params import load_params_from_json

    p = load_params_from_json(fname)
    assert p.params["λ"] == pytest.approx(params_1.params["λ"])


def test_case_statements(spark, sqlite_con_3):

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

    assert _check_jaro_registered(spark) == True

    dfpd = pd.read_sql("select * from str_comp", sqlite_con_3)
    df = spark.createDataFrame(dfpd)
    df.createOrReplaceTempView("str_comp")

    case_statement = sql_gen_case_stmt_levenshtein_3("str_col", "str_col")
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0, "gamma_str_col"] == 2
    assert df.loc[1, "gamma_str_col"] == 1
    assert df.loc[2, "gamma_str_col"] == 0
    assert df.loc[3, "gamma_str_col"] == -1
    assert df.loc[4, "gamma_str_col"] == -1

    case_statement = sql_gen_case_stmt_levenshtein_4("str_col", "str_col")
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0, "gamma_str_col"] == 3
    assert df.loc[1, "gamma_str_col"] == 2
    assert df.loc[2, "gamma_str_col"] == 0
    assert df.loc[3, "gamma_str_col"] == -1
    assert df.loc[4, "gamma_str_col"] == -1

    case_statement = sql_gen_gammas_case_stmt_jaro_2("str_col", "str_col")
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0, "gamma_str_col"] == 1
    assert df.loc[1, "gamma_str_col"] == 1
    assert df.loc[2, "gamma_str_col"] == 0
    assert df.loc[3, "gamma_str_col"] == -1
    assert df.loc[4, "gamma_str_col"] == -1

    case_statement = sql_gen_gammas_case_stmt_jaro_3("str_col", "str_col")
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0, "gamma_str_col"] == 2
    assert df.loc[1, "gamma_str_col"] == 2
    assert df.loc[2, "gamma_str_col"] == 0
    assert df.loc[3, "gamma_str_col"] == -1
    assert df.loc[4, "gamma_str_col"] == -1

    case_statement = sql_gen_gammas_case_stmt_jaro_4(
        "str_col", "str_col", threshold3=0.001
    )
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0, "gamma_str_col"] == 3
    assert df.loc[1, "gamma_str_col"] == 3
    assert df.loc[2, "gamma_str_col"] == 1
    assert df.loc[3, "gamma_str_col"] == -1
    assert df.loc[4, "gamma_str_col"] == -1

    
    data = [
        {"surname_l": "smith", "forename1_l": "john", "forename2_l": "david", 
         "surname_r": "smith", "forename1_r": "john", "forename2_r": "david"},

         {"surname_l": "smith", "forename1_l": "john", "forename2_l": "david", 
         "surname_r": "smithe", "forename1_r": "john", "forename2_r": "david"},

        {"surname_l": "smith", "forename1_l": "john", "forename2_l": "david", 
         "surname_r": "john", "forename1_r": "smith", "forename2_r": "david"},

        {"surname_l": "smith", "forename1_l": "john", "forename2_l": "david", 
         "surname_r": "john", "forename1_r": "david", "forename2_r": "smithe"},

        {"surname_l": "linacre", "forename1_l": "john", "forename2_l": "david", 
         "surname_r": "linaker", "forename1_r": "john", "forename2_r": "david"},

        {"surname_l": "smith", "forename1_l": "john", "forename2_l": "david", 
         "surname_r": "john", "forename1_r": "david", "forename2_r": "smarty"}
    ]
    dfpd = pd.DataFrame(data)
    df = spark.createDataFrame(dfpd)
    df.createOrReplaceTempView("df_names")

    sql = sql_gen_gammas_name_inversion_4("surname", ["forename1", "forename2"], "surname")

    df_results = spark.sql(f"select {sql} from df_names").toPandas()
    assert df_results.loc[0, "gamma_surname"] == 3
    assert df_results.loc[1, "gamma_surname"] == 3
    assert df_results.loc[2, "gamma_surname"] == 2
    assert df_results.loc[3, "gamma_surname"] == 2
    assert df_results.loc[4, "gamma_surname"] == 1    
    assert df_results.loc[5, "gamma_surname"] == 0
    
    
    


from splink.gammas import add_gammas


def test_iteration_known_data_generating_process(
    spark, gamma_settings_4, params_4, sqlite_con_4
):

    dfpd = pd.read_sql("select * from df", sqlite_con_4)

    df_gammas = spark.createDataFrame(dfpd)

    gamma_settings_4["retain_matching_columns"] = False
    gamma_settings_4["em_convergence"] = 0.001
    gamma_settings_4["max_iterations"] = 40
    df_e = iterate(
        df_gammas,
        params_4,
        gamma_settings_4,
        spark,
        compute_ll=False,
    )


    assert params_4.iteration < 20

    assert params_4.params["π"]["gamma_col_2_levels"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.05, abs=0.01)
    assert params_4.params["π"]["gamma_col_5_levels"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.1, abs=0.01)
    assert params_4.params["π"]["gamma_col_20_levels"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.05, abs=0.01)

    assert params_4.params["π"]["gamma_col_2_levels"]["prob_dist_non_match"]["level_1"][
        "probability"
    ] == pytest.approx(0.05, abs=0.01)
    assert params_4.params["π"]["gamma_col_5_levels"]["prob_dist_non_match"]["level_1"][
        "probability"
    ] == pytest.approx(0.2, abs=0.01)
    assert params_4.params["π"]["gamma_col_20_levels"]["prob_dist_non_match"][
        "level_1"
    ]["probability"] == pytest.approx(0.5, abs=0.01)


def test_link_option_link_dedupe(spark, link_dedupe_data_repeat_ids):
    settings = {
        "link_type": "link_and_dedupe",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}],
        "blocking_rules": [
            "l.first_name = r.first_name",
            "l.surname = r.surname"
        ]
    }
    settings = complete_settings_dict(settings, spark=None)
    dfpd_l = pd.read_sql("select * from df_l", link_dedupe_data_repeat_ids)
    df_l = spark.createDataFrame(dfpd_l)
    dfpd_r = pd.read_sql("select * from df_r", link_dedupe_data_repeat_ids)
    df_r = spark.createDataFrame(dfpd_r)
    df = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df.toPandas()
    df["u_l"] = df["unique_id_l"].astype(str) + df["_source_table_l"].str.slice(0,1)
    df["u_r"] = df["unique_id_r"].astype(str) + df["_source_table_r"].str.slice(0,1)
    df = df.sort_values(["_source_table_l", "_source_table_r", "unique_id_l", "unique_id_r"])

    assert list(df["u_l"]) == ['2l', '1l', '1l', '2l', '2l', '3l', '3l', '1r', '2r']
    assert list(df["u_r"]) == ['3l', '1r', '3r', '2r', '3r', '2r', '3r', '3r', '3r']

    # Same for no blocking rules = cartesian product

    settings = {
        "link_type": "link_and_dedupe",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}],
        "blocking_rules": [
        ]
    }
    settings = complete_settings_dict(settings, spark=None)
    dfpd_l = pd.read_sql("select * from df_l", link_dedupe_data_repeat_ids)
    df_l = spark.createDataFrame(dfpd_l)
    dfpd_r = pd.read_sql("select * from df_r", link_dedupe_data_repeat_ids)
    df_r = spark.createDataFrame(dfpd_r)
    df = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df.toPandas()

    df["u_l"] = df["unique_id_l"].astype(str) + df["_source_table_l"].str.slice(0,1)
    df["u_r"] = df["unique_id_r"].astype(str) + df["_source_table_r"].str.slice(0,1)
    df = df.sort_values(["_source_table_l", "unique_id_l","_source_table_r",  "unique_id_r"])

    assert list(df["u_l"]) == ['1l', '1l', '1l', '1l', '1l', '2l', '2l', '2l', '2l', '3l', '3l', '3l', '1r', '1r', '2r']
    assert list(df["u_r"]) == ['2l', '3l', '1r', '2r', '3r', '3l', '1r', '2r', '3r', '1r', '2r', '3r', '2r', '3r', '3r']



    # Same for cartesian product

    settings = {
        "link_type": "link_and_dedupe",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}]
    }
    settings = complete_settings_dict(settings, spark=None)
    dfpd_l = pd.read_sql("select * from df_l", link_dedupe_data_repeat_ids)
    df_l = spark.createDataFrame(dfpd_l)
    dfpd_r = pd.read_sql("select * from df_r", link_dedupe_data_repeat_ids)
    df_r = spark.createDataFrame(dfpd_r)
    df = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df.toPandas()
    df["u_l"] = df["unique_id_l"].astype(str) + df["_source_table_l"].str.slice(0,1)
    df["u_r"] = df["unique_id_r"].astype(str) + df["_source_table_r"].str.slice(0,1)
    df = df.sort_values(["_source_table_l", "unique_id_l","_source_table_r",  "unique_id_r"])

    assert list(df["u_l"]) == ['1l', '1l', '1l', '1l', '1l', '2l', '2l', '2l', '2l', '3l', '3l', '3l', '1r', '1r', '2r']
    assert list(df["u_r"]) == ['2l', '3l', '1r', '2r', '3r', '3l', '1r', '2r', '3r', '1r', '2r', '3r', '2r', '3r', '3r']

def test_link_option_link(spark, link_dedupe_data_repeat_ids):
    settings = {
        "link_type": "link_only",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}],
        "blocking_rules": [
            "l.first_name = r.first_name",
            "l.surname = r.surname"
        ]
    }
    settings = complete_settings_dict(settings, spark=None)
    dfpd_l = pd.read_sql("select * from df_l", link_dedupe_data_repeat_ids)
    df_l = spark.createDataFrame(dfpd_l)
    dfpd_r = pd.read_sql("select * from df_r", link_dedupe_data_repeat_ids)
    df_r = spark.createDataFrame(dfpd_r)
    df = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df.toPandas()

    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1, 1, 2, 2, 3, 3]
    assert list(df["unique_id_r"]) == [1, 3, 2, 3, 2, 3]

    # Test cartesian version

    settings = {
        "link_type": "link_only",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}],
        "blocking_rules": [

        ]
    }
    settings = complete_settings_dict(settings, spark=None)
    dfpd_l = pd.read_sql("select * from df_l", link_dedupe_data_repeat_ids)
    df_l = spark.createDataFrame(dfpd_l)
    dfpd_r = pd.read_sql("select * from df_r", link_dedupe_data_repeat_ids)
    df_r = spark.createDataFrame(dfpd_r)
    df = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df.toPandas()

    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert list(df["unique_id_r"]) == [1, 2, 3, 1, 2, 3, 1, 2, 3]



def test_link_option_dedupe_only(spark, link_dedupe_data_repeat_ids):
    settings = {
        "link_type": "dedupe_only",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}],
        "blocking_rules": [
            "l.first_name = r.first_name",
            "l.surname = r.surname"
        ]
    }
    settings = complete_settings_dict(settings, spark=None)
    dfpd = pd.read_sql("select * from df_l", link_dedupe_data_repeat_ids)
    df = spark.createDataFrame(dfpd)

    df = block_using_rules(settings, spark, df=df)
    df = df.toPandas()

    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [2]
    assert list(df["unique_id_r"]) == [3]


def test_main_api(spark, sqlite_con_1):

    settings = {
        "link_type": "dedupe_only",
        "comparison_columns": [{"col_name": "surname"},
                            {"col_name": "mob"}],
        "blocking_rules": ["l.mob = r.mob", "l.surname = r.surname"],
        "max_iterations": 2
    }
    settings = complete_settings_dict(settings, spark=None)
    dfpd = pd.read_sql("select * from test1", sqlite_con_1)

    df = spark.createDataFrame(dfpd)

    linker = Splink(settings,spark, df=df)
    df_e = linker.get_scored_comparisons()
    linker.save_model_as_json("saved_model.json", overwrite=True)
    linker_2 = load_from_json("saved_model.json", spark=spark, df=df)
    df_e = linker_2.get_scored_comparisons()

    from splink.intuition import intuition_report
    params = linker.params
    row_dict = df_e.toPandas().sample(1).to_dict(orient="records")[0]
    print(intuition_report(row_dict, params))

    linker.params._print_m_u_probs()



def test_term_frequency_adjustments(spark):


    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.1,
        "comparison_columns": [
            {
                "col_name": "name",
                "term_frequency_adjustments": True,
                "m_probabilities": [
                    0.1, # Amonst matches, 10% are have typose
                    0.9 # The reamining 90% have a match
                ],
                "u_probabilities": [
                    4/5, # Among non matches, 80% of the time there's no match
                    1/5 # But 20% of the time names 'collide'  WE WANT THESE U PROBABILITIES TO BE DEPENDENT ON NAME.  
                ],
            },
            {
                "col_name": "cat_12",
                "m_probabilities": [
                    0.05,
                    0.95
                ],
                "u_probabilities": [
                    11/12,
                    1/12
                ],
                
            },
            {
                "col_name": "cat_20",
                "m_probabilities": [
                    0.2,
                    0.8
                ],
                "u_probabilities": [
                    19/20,
                    1/20
                ],
            }
        ],
        "em_convergence": 0.001
    }


    from string import ascii_letters
    import statistics
    import random
    from splink.settings import complete_settings_dict
    settings = complete_settings_dict(settings, spark="supress_warnings")
    def is_match(settings):
        p = settings["proportion_of_matches"]
        return random.choices([0,1], [1-p, p])[0]

    def get_row_portion(match, comparison_col, skew="auto"):
        # Problem is that at the moment we're guaranteeing that a match on john is just as likely to be a match as a match on james
        
        # What we want is to generate more 'collisions' for john than robin i.e. if it's a non match, we want more gamma = 1 on name for john

        if match:
            gamma_pdist = comparison_col["m_probabilities"]
        else:
            gamma_pdist = comparison_col["u_probabilities"]
            
        
        # To decide whether gamma = 0 or 1 in the case of skew, we first need to decide on what value the left hand value column will take (well, what probability it has of selection)
        
        # How many distinct values should be choose?
        num_values = int(round(1/comparison_col["u_probabilities"][1]))
        
        if skew == "auto":
            skew = comparison_col["term_frequency_adjustments"]
            
        if skew:

            prob_dist = range(1,num_values+1)[::-1]  # a most freqent, last value least frequent
            # Normalise
            prob_dist = [p/sum(prob_dist) for p in prob_dist]
            
        
            index_of_value = random.choices(range(num_values), prob_dist)[0]
            if not match: # If it's a u probability
                this_prob = prob_dist[index_of_value]
                gamma_pdist = [1-this_prob, this_prob]
        
        else:
            prob_dist = [1/num_values]*num_values
            index_of_value = random.choices(range(num_values), prob_dist)[0]
            
            
        levels = comparison_col["num_levels"]
        gamma = random.choices(range(levels), gamma_pdist)[0]
        
        
        values = ascii_letters[:26] 
        if num_values > 26:
            values = [a + b for a in ascii_letters[:26] for b in ascii_letters[:26]] #aa, ab etc
            
        values = values[:num_values]

        if gamma == 1:
            value_1 = values[index_of_value]
            value_2 = value_1
            
        if gamma == 0:
            value_1 = values[index_of_value]
            same_value = True
            while same_value:
                value_2 = random.choices(values, prob_dist)[0]
                if value_1 != value_2:
                    same_value = False
            
        cname = comparison_col["col_name"]
        return {
            f"{cname}_l": value_1,
            f"{cname}_r": value_2,
            f"gamma_{cname}": gamma
        }    
        



    import uuid
    rows = []
    for uid in range(100000):
        m = is_match(settings)
        row = {"unique_id_l": str(uuid.uuid4()), "unique_id_r": str(uuid.uuid4()),  "match": m}
        for cc in settings["comparison_columns"]:
            row_portion = get_row_portion(m, cc)
            row = {**row, **row_portion}
        rows.append(row) 

    all_rows = pd.DataFrame(rows)
    df_gammas = spark.createDataFrame(all_rows)
    
    settings["comparison_columns"][1]["term_frequency_adjustments"] = True


    from splink import Splink
    from splink.params import Params 
    from splink.iterate import iterate
    from splink.term_frequencies import make_adjustment_for_term_frequencies

    # We have table of gammas - need to work from there within splink
    params = Params(settings, spark)

    df_e = iterate(
            df_gammas,
            params,
            settings,
            spark,
            compute_ll=False
        )

    df_e_adj = make_adjustment_for_term_frequencies(
            df_e,
            params,
            settings,
            retain_adjustment_columns=True,
            spark=spark
        )


    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select name_l, name_tf_adj,  count(*)
    from df_e_adj
    where name_l = name_r
    group by name_l, name_tf_adj
    order by name_l
    """
    df = spark.sql(sql).toPandas()
    df = df.set_index("name_l")
    df_dict = df.to_dict(orient='index')
    assert df_dict['a']["name_tf_adj"] < 0.5
    
    assert df_dict['e']["name_tf_adj"] > 0.5
    assert df_dict['e']["name_tf_adj"] > 0.6  #Arbitrary numbers, but we do expect a big uplift here
    assert df_dict['e']["name_tf_adj"] < 0.95 #Arbitrary numbers, but we do expect a big uplift here
    


    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select cat_12_l, cat_12_tf_adj,  count(*) as count
    from df_e_adj
    where cat_12_l = cat_12_r
    group by cat_12_l, cat_12_tf_adj
    order by cat_12_l
    """
    spark.sql(sql).toPandas()
    df = spark.sql(sql).toPandas()
    assert df["cat_12_tf_adj"].max() < 0.55 # Keep these loose because when generating random data anything can happen!
    assert df["cat_12_tf_adj"].min() > 0.45
    

    # Test adjustments applied coorrectly when there is one
    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select *
    from df_e_adj
    where name_l = name_r and cat_12_l != cat_12_r
    limit 1
    """
    df = spark.sql(sql).toPandas()
    df_dict = df.loc[0,:].to_dict()

    def bayes(p1, p2):
        return p1*p2 / (p1*p2 + (1-p1)*(1-p2))

    assert df_dict["tf_adjusted_match_prob"] ==  pytest.approx(bayes(df_dict["match_probability"], df_dict["name_tf_adj"]))
    

    # Test adjustments applied coorrectly when there are multiple
    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select *
    from df_e_adj
    where name_l = name_r and cat_12_l = cat_12_r
    limit 1
    """
    df = spark.sql(sql).toPandas()
    df_dict = df.loc[0,:].to_dict()

    double_b = bayes(bayes(df_dict["match_probability"], df_dict["name_tf_adj"]), df_dict["cat_12_tf_adj"])
    
    assert df_dict["tf_adjusted_match_prob"] ==  pytest.approx(double_b) 