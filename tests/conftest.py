import logging

from psycopg2 import connect
import pytest
from sqlalchemy import create_engine

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


@pytest.fixture(scope="session")
def pg_engine():

    engine = create_engine(f"postgresql://splinkognito:splink123!@localhost:5432/splink_db")
    yield engine

    engine.dispose()


@pytest.fixture(scope="session")
def pg_conn(pg_engine):
    pg_conn = pg_engine.connect()
    yield connect(
        dbname="splink_db",
        user="splinkognito",
        password="splink123!",
        host="localhost",
        port="5432",
    )
    pg_conn.close()
