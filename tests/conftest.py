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


def _make_spark(scratch_dir, driver_memory):
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    from splink.internals.spark.jar_location import similarity_jar_location

    conf = SparkConf()

    conf.set("spark.driver.memory", driver_memory)
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
    # Constraint propagation across Union nodes is buggy when combined with
    # checkpoint break-lineage (NoSuchElementException rewriting constraints);
    # it is a planner-only optimisation with no benefit on tiny test data, so
    # disabling it lets us use the faster checkpoint method safely.
    conf.set("spark.sql.constraintPropagation.enabled", "false")
    # Add custom similarity functions, which are bundled with Splink
    # documented here: https://github.com/moj-analytical-services/splink_scalaudfs
    path = similarity_jar_location()
    conf.set("spark.jars", path)

    # Keep the warehouse, Derby metastore and checkpoints inside a scratch
    # directory that pytest makes unique per worker (see `_spark_session`), so
    # parallel SparkSessions never collide on e.g. the Derby lock used by
    # saveAsTable.
    conf.set("spark.sql.warehouse.dir", str(scratch_dir / "warehouse"))
    conf.set(
        "spark.driver.extraJavaOptions",
        f"-Dderby.system.home={scratch_dir / 'derby'}",
    )

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    spark.sparkContext.setCheckpointDir(str(scratch_dir / "checkpoints"))

    return spark


def _reset_spark_session_state(spark):
    # The Spark session is shared across the whole run for speed, so per-test
    # catalog mutations must be undone explicitly. Clearing the
    # cache, resetting the current database and dropping temp views is cheap and
    # keeps each test isolated. Resetting the database before listing tables means
    # we can still clean up even if a test left a now-dropped database selected.
    spark.catalog.clearCache()
    spark.catalog.setCurrentDatabase("default")
    for table in spark.catalog.listTables():
        if table.isTemporary:
            spark.catalog.dropTempView(table.name)


@pytest.fixture(scope="session")
def _spark_session(tmp_path_factory, worker_id):
    # Owns the lifecycle of the single, shared Spark session: created lazily on
    # first use (so non-Spark runs never start a JVM) and stopped once at the end.
    # `tmp_path_factory` gives each pytest-xdist worker its own scratch directory;
    # cap the driver heap when running in parallel so several JVMs fit in memory.
    scratch_dir = tmp_path_factory.mktemp("spark")
    driver_memory = "6g" if worker_id == "master" else "2g"
    spark = _make_spark(scratch_dir, driver_memory)
    yield spark
    spark.stop()


@pytest.fixture
def spark(_spark_session):
    yield _spark_session
    _reset_spark_session_state(_spark_session)


# TODO: align this with test_helper
@pytest.fixture(scope="function")
def spark_api(spark):
    from splink.internals.spark.database_api import SparkAPI

    yield SparkAPI(spark_session=spark, num_partitions_on_repartition=1)


@pytest.fixture(scope="module")
def df_spark(_spark_session):
    df = _spark_session.read.csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )
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
    # For Spark we reuse the single, shared Spark session rather than creating
    # (and, expensively, stopping) a SparkContext for every test. It is resolved
    # lazily via request.getfixturevalue, so non-Spark dialects still never pay to
    # start Spark.
    def _shared_spark():
        return request.getfixturevalue("spark")

    helper_dict = LazyDict(
        duckdb=(DuckDBTestHelper, []),
        spark=(SparkTestHelper, [_shared_spark]),
        sqlite=(SQLiteTestHelper, []),
        postgres=(PostgresTestHelper, [pg_engine]),
    )
    yield helper_dict
    # The session-scoped `_spark_session` fixture owns the Spark lifecycle, and the
    # function-scoped `spark` fixture handles per-test catalog cleanup, so nothing
    # Spark-specific is needed here.
