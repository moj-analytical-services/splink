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

