import pandas as pd
import pytest
from snowflake.connector import SnowflakeConnection

from splink.backends.snowflake import SnowflakeAPI
from splink.datasets import splink_datasets


@pytest.fixture
def snowflake_connection() -> SnowflakeConnection:
    return SnowflakeConnection("splink_dev")


def test_snowflake_create_dp_api(snowflake_connection: SnowflakeConnection):
    _ = SnowflakeAPI(connection=snowflake_connection, register_udfs=False)


def test_snowflake__table_registration_pd_dataframe(
    snowflake_connection: SnowflakeConnection,
):
    conn = snowflake_connection
    cli = SnowflakeAPI(snowflake_connection, False)

    # Typing incomplete - ignore
    ds: pd.DataFrame = splink_datasets.fake_1000  # pyright: ignore[reportAssignmentType]

    cli._table_registration(ds, "FAKE_1000")

    res = (
        conn.cursor()
        .execute(
            "SELECT ROW_COUNT "
            "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'FAKE_1000'"
        )
        .fetchall()  # type: ignore -- May be None, we don't care
    )

    assert len(res) == 1, "should only have one row"
    assert res[0][0] == 1000, "should have 1000 rows"

    # Clean up
    _ = conn.cursor().execute("DROP TABLE FAKE_1000;")
