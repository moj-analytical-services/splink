import logging

import pytest

from splink.internals.spark.database_api import SparkAPI
from splink.internals.spark.jar_location import similarity_jar_location

# ruff: noqa: F401
# imported fixtures:
from tests.backend_utils.postgres_conf import (
    _engine_factory,
    _pg_credentials,
    _postgres,
    pg_engine,
)
from tests.decorator import dialect_groups
from tests.helpers import (
    DuckDBTestHelper,
    LazyDict,
    PostgresTestHelper,
    SparkTestHelper,
    SQLiteTestHelper,
)

logger = logging.getLogger(__name__)


def pytest_collection_modifyitems(items, config):
    # any tests without backend-group markers will always run
    marks = {gp for groups in dialect_groups.values() for gp in groups}
    # any mark we've added, but excluding e.g. parametrize
    our_marks = {*marks, *dialect_groups.keys()}

    for item in items:
        if not any(marker.name in our_marks for marker in item.iter_markers()):
            item.add_marker("core")
            for mark in our_marks:
                item.add_marker(mark)


def _make_spark():
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    conf = SparkConf()

    conf.set("spark.driver.memory", "6g")
    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.default.parallelism", "1")
    # Add custom similarity functions, which are bundled with Splink
    # documented here: https://github.com/moj-analytical-services/splink_scalaudfs
    path = similarity_jar_location()
    conf.set("spark.jars", path)

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    spark.sparkContext.setCheckpointDir("./tmp_checkpoints")
    return spark


def _cleanup_spark(spark):
    spark.catalog.clearCache()
    spark.stop()
    return


@pytest.fixture(scope="module")
def spark():
    spark = _make_spark()
    yield spark
    _cleanup_spark(spark)


# TODO: align this with test_helper
@pytest.fixture(scope="function")
def spark_api(spark):
    yield SparkAPI(spark_session=spark, num_partitions_on_repartition=1)


@pytest.fixture(scope="module")
def df_spark(spark):
    df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)
    df.persist()
    yield df


# workaround as you can't pass fixtures as param arguments in base pytest
# see e.g. https://stackoverflow.com/a/42400786/11811947
# ruff: noqa: F811
@pytest.fixture
def test_helpers(pg_engine):
    # LazyDict to lazy-load helpers
    # That way we do not instantiate helpers we do not need
    # e.g. running only duckdb tests we don't need PostgresTestHelper
    # so we can run duckdb tests in environments w/o access to postgres
    helper_dict = LazyDict(
        duckdb=(DuckDBTestHelper, []),
        spark=(SparkTestHelper, [_make_spark]),
        sqlite=(SQLiteTestHelper, []),
        postgres=(PostgresTestHelper, [pg_engine]),
    )
    yield helper_dict
    # if someone accessed spark, cleanup!
    if "spark" in helper_dict.accessed:
        _cleanup_spark(helper_dict["spark"].spark)


# Function to easily see if the gamma column added to the linker matches
# With the sets of tuples provided
@pytest.fixture(scope="module")
def test_gamma_assert():
    def _test_gamma_assert(linker_output, size_gamma_lookup, col_name):
        for gamma, id_pairs in size_gamma_lookup.items():
            for left, right in id_pairs:
                assert (
                    linker_output.loc[
                        (linker_output.unique_id_l == left)
                        & (linker_output.unique_id_r == right)
                    ]["gamma_" + col_name].values[0]
                    == gamma
                )

    return _test_gamma_assert
