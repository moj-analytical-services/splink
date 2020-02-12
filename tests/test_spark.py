# Derivations for a lot of the expected answers here:
# https://github.com/moj-analytical-services/sparklink/blob/dev/tests/expectation_maximisation_test_answers.xlsx
import copy
import os

from sparklink.blocking import cartestian_block, block_using_rules
from sparklink.params import Params
from sparklink.gammas import add_gammas
from sparklink.iterate import iterate
from sparklink.expectation_step import run_expectation_step, get_overall_log_likelihood
from sparklink.case_statements import *
from sparklink.case_statements import _check_jaro_registered
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import logging

log = logging.getLogger(__name__)


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
        conf.set('spark.driver.extraClassPath', 'jars/scala-udf-similarity-0.0.6.jar')
        conf.set('spark.jars', 'jars/scala-udf-similarity-0.0.6.jar')
        conf.set('spark.driver.memory', '4g')
        conf.set("spark.sql.shuffle.partitions", "24")

        sc = SparkContext.getOrCreate(conf=conf)

        spark = SparkSession(sc)

        udfs = [
            ('jaro_winkler_sim', 'JaroWinklerSimilarity',types.DoubleType()),
        ('jaccard_sim', 'JaccardSimilarity',types.DoubleType()),
        ('cosine_distance', 'CosineDistance',types.DoubleType()),
        ('Dmetaphone', 'DoubleMetaphone',types.StringType()),
        ('QgramTokeniser', 'QgramTokeniser',types.StringType()),
        ('Q3gramTokeniser','Q3gramTokeniser',types.StringType()),
        ('Q4gramTokeniser','Q4gramTokeniser',types.StringType()),
        ('Q5gramTokeniser','Q5gramTokeniser',types.StringType())
        ]

        for a,b,c in udfs:
            spark.udf.registerJavaFunction(a, 'uk.gov.moj.dash.linkage.'+ b, c)
        SPARK_EXISTS = True
    except:
        SPARK_EXISTS = False

    if SPARK_EXISTS:
        print("Spark exists, running spark tests")
        yield spark
    else:
        spark = None
        log.error("Spark not available")
        print("Spark not available")
        yield spark


from pyspark.sql import SparkSession, Row


def test_expectation(spark, sqlite_con_1, params_1, gamma_settings_1):
    dfpd = pd.read_sql("select * from test1", sqlite_con_1)
    df = spark.createDataFrame(dfpd)

    rules = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    df_comparison = block_using_rules(df, rules, spark=spark)

    df_gammas = add_gammas(
        df_comparison, gamma_settings_1, spark, include_orig_cols=False
    )

    # df_e = iterate(df_gammas, spark, params_1, num_iterations=1)
    df_e = run_expectation_step(df_gammas, spark, params_1)

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

    # Regression test, see https://github.com/moj-analytical-services/sparklink/issues/48

    dfpd = pd.read_sql("select * from test1", sqlite_con_1)
    df = spark.createDataFrame(dfpd)

    rules = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    gamma_settings = {
        "proportion_of_matches" : 0.4,
        "comparison_columns" : [
    {"col_name": "mob",
        "num_levels": 2,
        "m_probabilities": [5.9380419956766985e-25, 1-5.9380419956766985e-25],
        "u_probabilities": [0.8, 0.2]

    },
    {"col_name":"surname",
        "num_levels": 2,
    }]}


    df_comparison = block_using_rules(df, rules, spark=spark)

    df_gammas = add_gammas(
        df_comparison, gamma_settings, spark, include_orig_cols=False
    )
    params = Params(gamma_settings, spark="supress_warnings")

    df_e = run_expectation_step(df_gammas, spark, params)

def test_iterate(spark, sqlite_con_1, params_1, gamma_settings_1):

    original_params = copy.deepcopy(params_1.params)
    dfpd = pd.read_sql("select * from test1", sqlite_con_1)
    df = spark.createDataFrame(dfpd)

    rules = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    df_comparison = block_using_rules(df, rules, spark=spark)

    df_gammas = add_gammas(
        df_comparison, gamma_settings_1, spark, include_orig_cols=False
    )


    df_e = iterate(df_gammas, spark, params_1, num_iterations=1)

    assert params_1.params["λ"] == pytest.approx(0.540922141)

    assert params_1.params["π"]["gamma_0"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.087438272, abs=0.0001)
    assert params_1.params["π"]["gamma_1"]["prob_dist_non_match"]["level_1"][
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

    df_e = iterate(df_gammas, spark, params_1, num_iterations=1)
    assert params_1.params["λ"] == pytest.approx(0.534993426, abs=0.0001)



    assert params_1.params["π"]["gamma_0"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.088546179, abs=0.0001)
    assert params_1.params["π"]["gamma_1"]["prob_dist_non_match"]["level_1"][
        "probability"
    ] == pytest.approx(0.109234086, abs=0.0001)

    ## Test whether the params object is correctly storing the iteration history

    assert params_1.param_history[0] == original_params
    assert params_1.param_history[1] == first_it_params

    ## Now test whether, when we

    data = params_1.convert_params_dict_to_data(original_params)
    val1 = {'gamma': 'gamma_0', 'match': 0, 'value_of_gamma': 'level_0', 'probability': 0.8, 'value': 0, 'column': 'mob'}
    val2 = {'gamma': 'gamma_1', 'match': 1, 'value_of_gamma': 'level_1', 'probability': 0.2, 'value': 1, 'column': 'surname'}

    assert val1 in data
    assert val2 in data

    correct_list = [
        {"iteration": 0, 'λ': 0.4},
        {"iteration": 1, 'λ': 0.540922141}
    ]

    result_list = params_1.iteration_history_df_lambdas()

    for i in zip(result_list, correct_list):
        assert i[0]["iteration"] == i[1]["iteration"]
        assert i[0]["λ"] == pytest.approx(i[1]['λ'])


    result_list = params_1.iteration_history_df_gammas()

    val1 = {'iteration': 0, 'gamma': 'gamma_0', 'match': 0, 'value_of_gamma': 'level_0', 'probability': 0.8, 'value': 0, 'column': 'mob'}
    assert val1 in result_list

    val2 = {'iteration': 1, 'gamma': 'gamma_1', 'match': 0, 'value_of_gamma': 'level_1', 'probability': 0.160167628, 'value': 1, 'column': 'surname'}

    for r in result_list:
        if r["iteration"] == 1:
            if r["gamma"] == 'gamma_1':
                if r["match"] == 0:
                    if r["value"] == 1:
                        record = r

    for k, v in record.items():
        expected_value = val2[k]
        if k == 'probability':
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
    print(fname)
    print(fname)
    print(fname)
    print(fname)
    from sparklink.params import load_params_from_json
    p = load_params_from_json(fname)
    assert p.params["λ"] == pytest.approx(params_1.params['λ'])

def test_case_statements(spark, sqlite_con_3):

    assert _check_jaro_registered(spark) == True

    spark.sql("drop temporary function jaro_winkler_sim")
    with pytest.warns(UserWarning):
        assert _check_jaro_registered(spark) == False
    from pyspark.sql.types import DoubleType
    spark.udf.registerJavaFunction('jaro_winkler_sim', 'uk.gov.moj.dash.linkage.JaroWinklerSimilarity', DoubleType())

    assert _check_jaro_registered(spark) == True

    dfpd = pd.read_sql("select * from str_comp", sqlite_con_3)
    df = spark.createDataFrame(dfpd)
    df.createOrReplaceTempView("str_comp")

    case_statement = sql_gen_case_stmt_levenshtein_3("str_col", 0)
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0,'gamma_0'] == 2
    assert df.loc[1,'gamma_0'] == 1
    assert df.loc[2,'gamma_0'] == 0
    assert df.loc[3,'gamma_0'] == -1
    assert df.loc[4,'gamma_0'] == -1

    case_statement = sql_gen_case_stmt_levenshtein_4("str_col", 0)
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0,'gamma_0'] == 3
    assert df.loc[1,'gamma_0'] == 2
    assert df.loc[2,'gamma_0'] == 0
    assert df.loc[3,'gamma_0'] == -1
    assert df.loc[4,'gamma_0'] == -1

    case_statement = sql_gen_gammas_case_stmt_jaro_2("str_col", 0)
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0,'gamma_0'] == 1
    assert df.loc[1,'gamma_0'] == 1
    assert df.loc[2,'gamma_0'] == 0
    assert df.loc[3,'gamma_0'] == -1
    assert df.loc[4,'gamma_0'] == -1

    case_statement = sql_gen_gammas_case_stmt_jaro_3("str_col", 0)
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0,'gamma_0'] == 2
    assert df.loc[1,'gamma_0'] == 2
    assert df.loc[2,'gamma_0'] == 0
    assert df.loc[3,'gamma_0'] == -1
    assert df.loc[4,'gamma_0'] == -1

    case_statement = sql_gen_gammas_case_stmt_jaro_4("str_col", 0, threshold3=0.001)
    sql = f"""select {case_statement} from str_comp"""
    df = spark.sql(sql).toPandas()

    assert df.loc[0,'gamma_0'] == 3
    assert df.loc[1,'gamma_0'] == 3
    assert df.loc[2,'gamma_0'] == 1
    assert df.loc[3,'gamma_0'] == -1
    assert df.loc[4,'gamma_0'] == -1


from sparklink.gammas import add_gammas

def test_iteration_known_data_generating_process(spark, gamma_settings_4, params_4, sqlite_con_4):

    dfpd = pd.read_sql("select * from df", sqlite_con_4)

    df_gammas = spark.createDataFrame(dfpd)

    df_e = iterate(df_gammas, spark, params_4, num_iterations=40, compute_ll=False)

    assert params_4.params["π"]["gamma_0"]["prob_dist_match"]["level_0"]["probability"] == pytest.approx(0.05, abs=0.002)
    assert params_4.params["π"]["gamma_1"]["prob_dist_match"]["level_0"]["probability"] == pytest.approx(0.1, abs=0.002)
    assert params_4.params["π"]["gamma_2"]["prob_dist_match"]["level_0"]["probability"] == pytest.approx(0.05, abs=0.002)


    assert params_4.params["π"]["gamma_0"]["prob_dist_non_match"]["level_1"]["probability"] == pytest.approx(0.05, abs=0.002)
    assert params_4.params["π"]["gamma_1"]["prob_dist_non_match"]["level_1"]["probability"] == pytest.approx(0.2, abs=0.002)
    assert params_4.params["π"]["gamma_2"]["prob_dist_non_match"]["level_1"]["probability"] == pytest.approx(0.5, abs=0.002)

