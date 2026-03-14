import logging
from typing import Any, Union

import pandas as pd
import snowflake.connector.pandas_tools as sf_pd_tools
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from splink.internals.database_api import DatabaseAPI
from splink.internals.dialects import SnowflakeDialect
from splink.internals.splink_dataframe import SplinkDataFrame

from .dataframe import SnowflakeDataframe

logger = logging.getLogger(__name__)


class SnowflakeAPI(DatabaseAPI[SnowflakeCursor]):
    sql_dialect = SnowflakeDialect()
    _con: SnowflakeConnection

    def __init__(self, connection: SnowflakeConnection, register_udfs: bool = True):
        super().__init__()

        self._con = connection
        self.__set_snowflake_quoted_identifiers_ignore()

        if register_udfs:
            self._register_udfs()

    def _register_udfs(self):
        # if people have issues with permissions we can allow these to be optional
        # need for predict_from_comparison_vectors_sql (could adjust)
        self._create_log2_function()

    def _create_log2_function(self):
        sql = """
            CREATE TEMPORARY FUNCTION IF NOT EXISTS LOG2(FLOAT_IN FLOAT)
            RETURNS FLOAT
            AS
            $$
            LOG(2, FLOAT_IN)
            $$;
            """
        self._con.cursor().execute(sql)

    def __set_snowflake_quoted_identifiers_ignore(self) -> None:
        logger.warning(
            "Setting snowflake session to ignore quoted identifiers for greater"
            " compatibility"
        )
        self._con.cursor().execute(
            "ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE;"
        )

    def _execute_sql_against_backend(self, final_sql: str) -> SnowflakeCursor:
        result = self._con.cursor().execute(final_sql)
        assert result is not None, "cursor.execute() returned None"
        return result

    def _table_registration(  # type: ignore -- Ignore more concrete types defined
        self, input: Union[dict, list, pd.DataFrame], table_name: str
    ) -> None:
        # Try and use same approach as for postgres where we
        # process everything as snowflake DataFrames
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # HACK: Force table names to be upper to allow more flexible use cases
        table_name = table_name.upper()

        # Use snowflake helper library rather than faff around
        # TODO: Maybe just import explicit libraries
        sf_pd_tools.write_pandas(self._con, input, table_name, auto_create_table=True)

    def table_to_splink_dataframe(
        self, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        return SnowflakeDataframe(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name: str) -> bool:
        # Use similar implementation to Postgres
        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = '{table_name.upper()}';
        """

        res = self._execute_sql_against_backend(sql).fetchall()

        return len(res) > 0
