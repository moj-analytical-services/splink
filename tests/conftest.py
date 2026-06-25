import logging
import re
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

# Holds the single, session-scoped Spark session once it has been created, so we
# can cheaply reset its transient catalog state between tests. It stays None
# until the first test that actually needs Spark, so non-Spark runs never start a
# JVM.
_active_spark_session = None


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
    import os

    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    from splink.internals.spark.jar_location import similarity_jar_location

    conf = SparkConf()

    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.default.parallelism", "1")
    # Disable the Spark UI: it isn't useful in CI, shaves a little off start-up,
    # and (importantly when running tests in parallel with pytest-xdist) avoids
    # several JVMs fighting over the UI port.
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.ui.showConsoleProgress", "false")
    # Adaptive query execution adds planning overhead that is pure cost on the
    # tiny datasets used in the test suite.
    conf.set("spark.sql.adaptive.enabled", "false")
    # Add custom similarity functions, which are bundled with Splink
    # documented here: https://github.com/moj-analytical-services/splink_scalaudfs
    path = similarity_jar_location()
    conf.set("spark.jars", path)

    # When running under pytest-xdist each worker is a separate process, but they
    # share a working directory. Give every worker its own warehouse / metastore /
    # checkpoint location so that concurrent SparkSessions don't collide (e.g. on
    # the embedded Derby metastore lock used by saveAsTable), and cap the driver
    # heap so that several JVMs fit within the runner's memory.
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        scratch = os.path.abspath(os.path.join("tmp_spark_scratch", worker))
        os.makedirs(scratch, exist_ok=True)
        conf.set("spark.driver.memory", "2g")
        conf.set("spark.sql.warehouse.dir", os.path.join(scratch, "warehouse"))
        conf.set(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home={os.path.join(scratch, 'derby')}",
        )
        checkpoint_dir = os.path.join(scratch, "checkpoints")
    else:
        conf.set("spark.driver.memory", "6g")
        checkpoint_dir = "./tmp_checkpoints"

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    global _active_spark_session
    _active_spark_session = spark
    return spark


def _reset_spark_session_state(spark):
    # The Spark session is shared across the whole test session (for speed), so
    # per-test catalog mutations must be undone explicitly. Previously this
    # happened as a (very expensive) side effect of calling spark.stop() after
    # every test. Here we just clear the cache, reset the current database and
    # drop temporary views, which is cheap but preserves per-test isolation.
    try:
        spark.catalog.clearCache()
    except Exception:
        pass
    try:
        spark.catalog.setCurrentDatabase("default")
    except Exception:
        pass
    try:
        for table in spark.catalog.listTables():
            if table.isTemporary:
                spark.catalog.dropTempView(table.name)
    except Exception:
        pass


def _cleanup_spark(spark):
    global _active_spark_session
    spark.catalog.clearCache()
    spark.stop()
    _active_spark_session = None
    return


@pytest.fixture(autouse=True)
def _reset_spark_state_between_tests():
    # Restore the per-test isolation that the old (slow) "stop Spark after every
    # test" approach gave us, without paying the cost of stopping Spark. Only does
    # anything once a Spark session has actually been started.
    yield
    if _active_spark_session is not None:
        _reset_spark_session_state(_active_spark_session)


@pytest.fixture(scope="session")
def spark():
    spark = _make_spark()
    yield spark
    _cleanup_spark(spark)


# TODO: align this with test_helper
@pytest.fixture(scope="function")
def spark_api(spark):
    from splink.internals.spark.database_api import SparkAPI

    yield SparkAPI(spark_session=spark, num_partitions_on_repartition=1)


@pytest.fixture(scope="module")
def df_spark(spark):
    df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)
    df.persist()
    yield df


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
    #
    # For Spark we reuse the single, session-scoped `spark` fixture rather than
    # creating (and, expensively, stopping) a SparkContext for every test. The
    # session is resolved lazily via request.getfixturevalue, so non-Spark
    # dialects still never pay to start Spark.
    def _shared_spark():
        return request.getfixturevalue("spark")

    helper_dict = LazyDict(
        duckdb=(DuckDBTestHelper, []),
        spark=(SparkTestHelper, [_shared_spark]),
        sqlite=(SQLiteTestHelper, []),
        postgres=(PostgresTestHelper, [pg_engine]),
    )
    yield helper_dict
    # The session-scoped `spark` fixture owns the Spark lifecycle, and the autouse
    # `_reset_spark_state_between_tests` fixture handles per-test catalog cleanup,
    # so nothing Spark-specific is needed here.
