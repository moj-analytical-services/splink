import logging
import os
import re
import shutil
import tempfile
from dataclasses import dataclass

import pytest

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


def _spark_worker_count() -> int:
    cpu_count = os.cpu_count() or 2
    return max(2, min(4, cpu_count))


def pytest_collection_modifyitems(items, config):
    # any tests without backend-group markers will always run
    marks = {gp for groups in dialect_groups.values() for gp in groups}
    # any mark we've added, but excluding e.g. parametrize
    our_marks = {*marks, *dialect_groups.keys()}

    for item in items:
        # Any test without backend-specific marker gets 'core' marker (by definition)
        # Also a dialect marker for each specified dialect
        # Does not get {dialect}_only, as these are specifically for non-core tests
        if not any(marker.name in our_marks for marker in item.iter_markers()):
            item.add_marker("core")
            for mark in our_marks:
                item.add_marker(mark)


def _make_spark():
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

    from splink.internals.spark.jar_location import similarity_jar_location

    worker_count = _spark_worker_count()
    base_tmp = os.environ.get("RUNNER_TEMP", tempfile.gettempdir())
    scratch_dir = tempfile.mkdtemp(prefix="splink-spark-", dir=base_tmp)
    checkpoint_dir = os.path.join(scratch_dir, "checkpoints")
    warehouse_dir = os.path.join(scratch_dir, "warehouse")

    os.makedirs(checkpoint_dir, exist_ok=True)
    os.makedirs(warehouse_dir, exist_ok=True)

    conf = SparkConf(False)
    conf.setMaster(f"local[{worker_count}]")
    conf.setAppName("splink-tests")
    conf.set("spark.default.parallelism", str(worker_count))
    conf.set("spark.sql.shuffle.partitions", str(worker_count * 2))
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.ui.showConsoleProgress", "false")
    conf.set("spark.local.dir", scratch_dir)
    conf.set("spark.sql.warehouse.dir", warehouse_dir)
    # Add custom similarity functions, which are bundled with Splink
    # documented here: https://github.com/moj-analytical-services/splink_scalaudfs
    conf.set("spark.jars", similarity_jar_location())

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    spark._splink_test_scratch_dir = scratch_dir
    return spark


def _cleanup_spark(spark):
    spark.catalog.clearCache()
    spark.stop()
    scratch_dir = getattr(spark, "_splink_test_scratch_dir", None)
    if scratch_dir is not None:
        shutil.rmtree(scratch_dir, ignore_errors=True)
    return


def _reset_spark_state(spark):
    spark.catalog.clearCache()
    for table in spark.catalog.listTables():
        if table.isTemporary:
            spark.catalog.dropTempView(table.name)

    try:
        for table in spark.catalog.listTables("global_temp"):
            if table.isTemporary:
                spark.catalog.dropGlobalTempView(table.name)
    except Exception:
        pass

    try:
        spark.catalog.setCurrentDatabase("default")
    except Exception:
        pass


@pytest.fixture(scope="session")
def spark():
    spark = _make_spark()
    yield spark
    _cleanup_spark(spark)


# TODO: align this with test_helper
@pytest.fixture(scope="function")
def spark_api(spark):
    from splink.internals.spark.database_api import SparkAPI

    yield SparkAPI(spark_session=spark, num_partitions_on_repartition=2)


@pytest.fixture(scope="session")
def df_spark(spark):
    df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)
    df = df.cache()
    df.count()
    yield df
    df.unpersist()


@pytest.fixture(scope="session")
def fake_1000():
    import pyarrow.csv as pv

    return pv.read_csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv",
        convert_options=pv.ConvertOptions(strings_can_be_null=True),
    )


@pytest.fixture(scope="function")
def duckdb_with_fake_1000():
    import duckdb

    con = duckdb.connect()
    fake_1000 = con.query(
        "select * "
        "from read_csv_auto('./tests/datasets/fake_1000_from_splink_demos.csv')"
    )

    @dataclass
    class DuckDB:
        con: duckdb.DuckDBPyConnection
        fake_1000: duckdb.DuckDBPyRelation

    return DuckDB(con, fake_1000)


@pytest.fixture
def unique_per_test_table_name(request):
    # postgres name type limits to 63. Truncate from right as more unique
    return re.sub(r"[^a-zA-Z0-9_]", "_", request.node.nodeid)[-63:]


# workaround as you can't pass fixtures as param arguments in base pytest
# see e.g. https://stackoverflow.com/a/42400786/11811947
# ruff: noqa: F811
@pytest.fixture
def test_helpers(pg_engine, request):
    # LazyDict to lazy-load helpers
    # That way we do not instantiate helpers we do not need
    # e.g. running only duckdb tests we don't need PostgresTestHelper
    # so we can run duckdb tests in environments w/o access to postgres
    helper_dict = LazyDict(
        duckdb=(DuckDBTestHelper, []),
        spark=(SparkTestHelper, [lambda: request.getfixturevalue("spark")]),
        sqlite=(SQLiteTestHelper, []),
        postgres=(PostgresTestHelper, [pg_engine]),
    )
    yield helper_dict


@pytest.fixture(autouse=True)
def _cleanup_shared_spark_state(request):
    yield

    uses_shared_spark = any(
        name in request.fixturenames for name in ("spark", "spark_api", "df_spark")
    )

    if not uses_shared_spark and "dialect" in request.fixturenames:
        uses_shared_spark = request.getfixturevalue("dialect") == "spark"

    if uses_shared_spark:
        _reset_spark_state(request.getfixturevalue("spark"))
