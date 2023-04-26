import logging

import pytest

from splink.spark.jar_location import similarity_jar_location

# TODO: maybe that should import from here?
from tests.decorator import dialect_groups
from tests.helpers import DuckDBTestHelper, SparkTestHelper, SQLiteTestHelper

logger = logging.getLogger(__name__)

# DESIRED BEHAVIOUR:
# two sets of tests - those with backend flags (D) and those without (A)
# all tests run (A). Additionally:
# pytest --- runs also 'default' backends - probably duckdb (+ spark?), where applicable
# # pytest -m duckdb ---runs (A) and only duckdb tests & sim for all backends
# pytest -m all --- runs (A) and all backend tests against all backends


def pytest_collection_modifyitems(items, config):
    # any tests without backend-group markers will always run
    marks = {gp for groups in dialect_groups.values() for gp in groups}
    # any mark we've added, but excluding e.g. parametrize
    our_marks = {*marks, *dialect_groups.keys()}

    for item in items:
        if not any(marker.name in our_marks for marker in item.iter_markers()):
            for mark in our_marks:
                item.add_marker(mark)


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


# does this even need to be a fixture?? :thinking_face:
@pytest.fixture
def duckdb_helper():
    return DuckDBTestHelper()


@pytest.fixture
def spark_helper(spark):
    return SparkTestHelper(spark)


@pytest.fixture
def sqlite_helper():
    return SQLiteTestHelper()


# workaround as you can't pass fixtures as param arguments in base pytest
# see e.g. https://stackoverflow.com/a/42400786/11811947
@pytest.fixture
def test_helpers(duckdb_helper, spark_helper, sqlite_helper):
    return {
        "duckdb": duckdb_helper,
        "spark": spark_helper,
        "sqlite": sqlite_helper,
    }
