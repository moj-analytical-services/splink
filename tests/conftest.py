import logging

import pytest

from splink.spark.jar_location import similarity_jar_location

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def spark():

    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    conf = SparkConf()

    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "8")
    conf.set("spark.default.parallelism", "8")

    # Add custom similarity functions, which are bundled with Splink
    # documented here: https://github.com/moj-analytical-services/splink_scalaudfs
    path = similarity_jar_location()
    conf.set("spark.jars", path)

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    spark.sparkContext.setCheckpointDir("./tmp_checkpoints")

    yield spark


@pytest.fixture(scope="module")
def df_spark(spark):
    df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)
    df.persist()
    yield df


@pytest.fixture(scope="module")
def first_name_and_surname_cc():
    # A comparison level made up of composition between first_name and surname
    def _first_name_and_surname_cc(cll, sn="surname"):
        fn_sn_cc = {
            "output_column_name": "first_name_and_surname",
            "comparison_levels": [
                # Null level
                cll.or_(cll.null_level("first_name"), cll.null_level(sn)),
                # Exact match on fn and sn
                cll.or_(
                    cll.exact_match_level("first_name"),
                    cll.exact_match_level(sn),
                    m_probability=0.8,
                    label_for_charts="Exact match on first name or surname",
                ),
                # (Levenshtein(fn) and jaro_winkler(fn)) or levenshtein(sur)
                cll.and_(
                    cll.or_(
                        cll.levenshtein_level("first_name", 2),
                        cll.jaro_winkler_level("first_name", 0.8),
                        m_probability=0.8,
                    ),
                    cll.levenshtein_level(sn, 3),
                ),
                cll.else_level(0.1),
            ],
        }
        return fn_sn_cc

    return _first_name_and_surname_cc
