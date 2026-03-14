from splink.backends.snowflake import SnowflakeAPI
from snowflake.connector import SnowflakeConnection


def test_snowflake_create_dataframe():
    conn = SnowflakeConnection("splink_dev")

    db_api = SnowflakeAPI(connection=conn, register_udfs=False)
