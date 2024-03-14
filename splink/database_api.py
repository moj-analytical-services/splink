import hashlib
import logging
import random
import time
from abc import ABC, abstractmethod
from typing import Dict, Generic, List, Optional, TypeVar, Union, final

import sqlglot

from .cache_dict_with_logging import CacheDictWithLogging
from .dialects import (
    SplinkDialect,
)
from .exceptions import SplinkException
from .logging_messages import execute_sql_logging_message_info, log_sql
from .misc import (
    ascii_uid,
    parse_duration,
)
from .splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


# a placeholder type. This will depend on the backend subclass - something
# 'tabley' for that backend, such as duckdb.DuckDBPyRelation or spark.DataFrame
TablishType = TypeVar("TablishType")


class DatabaseAPI(ABC, Generic[TablishType]):
    sql_dialect: SplinkDialect
    debug_mode: bool = False
    """
    DatabaseAPI class handles _all_ interactions with the database
    Anything backend-specific (but not related to SQL dialects) lives here also

    This is intended to be subclassed for specific backends
    """

    def __init__(self) -> None:
        self._intermediate_table_cache: CacheDictWithLogging = CacheDictWithLogging()
        # TODO: replace this:
        self._cache_uid: str = str(random.choice(range(10000)))

    @final
    def _log_and_run_sql_execution(
        self, final_sql: str, templated_name: str, physical_name: str
    ) -> TablishType:
        """
        Log some sql, then call _run_sql_execution()
        Any errors will be converted to SplinkException with more detail
        names are only relevant for logging, not execution
        """
        logger.debug(execute_sql_logging_message_info(templated_name, physical_name))
        logger.log(5, log_sql(final_sql))
        try:
            return self._execute_sql_against_backend(final_sql)
        except Exception as e:
            # Parse our SQL through sqlglot to pretty print
            try:
                final_sql = sqlglot.parse_one(
                    final_sql,
                    read=self.sql_dialect,
                ).sql(pretty=True)
                # if sqlglot produces any errors, just report the raw SQL
            except Exception:
                pass

            raise SplinkException(
                f"Error executing the following sql for table "
                f"`{templated_name}`({physical_name}):\n{final_sql}"
                f"\n\nError was: {e}"
            ) from e

    # TODO: rename this?
    @final
    def _sql_to_splink_dataframe(
        self, sql: str, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        """
        Create a table in the backend using some given sql

        Table will have physical_name in the backend.

        Returns a SplinkDataFrame which also uses templated_name
        """
        sql = self._setup_for_execute_sql(sql, physical_name)
        spark_df = self._log_and_run_sql_execution(sql, templated_name, physical_name)
        output_df = self._cleanup_for_execute_sql(
            spark_df, templated_name, physical_name
        )
        self._intermediate_table_cache.executed_queries.append(output_df)
        return output_df

    @final
    def _get_table_from_cache_or_db(
        self, table_name_hash: str, output_tablename_templated: str
    ) -> Union[SplinkDataFrame, None]:
        # Certain tables are put in the cache using their templated_name
        # An example is __splink__df_concat_with_tf
        # These tables are put in the cache when they are first calculated
        # e.g. with _initialise_df_concat_with_tf()
        # But they can also be put in the cache manually using
        # e.g. register_table_input_nodes_concat_with_tf()

        # Look for these 'named' tables in the cache prior
        # to looking for the hashed version s
        if output_tablename_templated in self._intermediate_table_cache:
            return self._intermediate_table_cache.get_with_logging(
                output_tablename_templated
            )

        if table_name_hash in self._intermediate_table_cache:
            return self._intermediate_table_cache.get_with_logging(table_name_hash)

        # If not in cache, fall back on checking the database
        if self.table_exists_in_database(table_name_hash):
            logger.debug(
                f"Found cache for {output_tablename_templated} "
                f"in database using table name with physical name {table_name_hash}"
            )
            return self.table_to_splink_dataframe(
                output_tablename_templated, table_name_hash
            )
        return None

    @final
    def sql_to_splink_dataframe_checking_cache(
        self,
        sql,
        output_tablename_templated,
        use_cache=True,
    ) -> SplinkDataFrame:
        # differences from _sql_to_splink_dataframe:
        # this _calculates_ physical name, and
        # handles debug_mode
        # TODO: also maybe caching? but maybe that is even lower down
        to_hash = (sql + self._cache_uid).encode("utf-8")
        hash = hashlib.sha256(to_hash).hexdigest()[:9]
        # Ensure hash is valid sql table name
        table_name_hash = f"{output_tablename_templated}_{hash}"

        if use_cache:
            splink_dataframe = self._get_table_from_cache_or_db(
                table_name_hash, output_tablename_templated
            )
            if splink_dataframe is not None:
                return splink_dataframe

        if self.debug_mode:
            print(sql)  # noqa: T201
            splink_dataframe = self._sql_to_splink_dataframe(
                sql,
                output_tablename_templated,
                output_tablename_templated,
            )

            df_pd = splink_dataframe.as_pandas_dataframe()
            try:
                from IPython.display import display

                display(df_pd)
            except ModuleNotFoundError:
                print(df_pd)  # noqa: T201

        else:
            splink_dataframe = self._sql_to_splink_dataframe(
                sql, output_tablename_templated, table_name_hash
            )

        splink_dataframe.created_by_splink = True
        splink_dataframe.sql_used_to_create = sql

        physical_name = splink_dataframe.physical_name

        self._intermediate_table_cache[physical_name] = splink_dataframe

        return splink_dataframe

    def sql_pipeline_to_splink_dataframe(
        self,
        pipeline,
        input_dataframes: List[SplinkDataFrame] = [],
        use_cache=True,
    ) -> SplinkDataFrame:
        """
        Execute a given pipeline using input_dataframes as seeds if provided.
        self.debug_mode controls whether this is CTE or individual tables.
        pipeline is resest upon completion
        """

        if not self.debug_mode:
            sql_gen = pipeline.generate_pipeline(input_dataframes)
            output_tablename_templated = pipeline.output_table_name

            splink_dataframe = self.sql_to_splink_dataframe_checking_cache(
                sql_gen,
                output_tablename_templated,
                use_cache,
            )
        else:
            # In debug mode, we do not pipeline the sql and print the
            # results of each part of the pipeline
            for task in pipeline.generate_pipeline_parts(input_dataframes):
                start_time = time.time()
                output_tablename = task.output_table_name
                sql = task.sql
                print("------")  # noqa: T201
                print(  # noqa: T201
                    f"--------Creating table: {output_tablename}--------"
                )

                splink_dataframe = self.sql_to_splink_dataframe_checking_cache(
                    sql,
                    output_tablename,
                    use_cache=False,
                )
                run_time = parse_duration(time.time() - start_time)
                print(f"Step ran in: {run_time}")  # noqa: T201

        # if there is an error the pipeline will not reset, leaving caller to handle
        pipeline.reset()
        return splink_dataframe

    @final
    def register_multiple_tables(
        self,
        input_tables,
        input_aliases: Optional[List[str]] = None,
        overwrite: bool = False,
    ) -> Dict[str, SplinkDataFrame]:

        tables_as_splink_dataframes = {}
        existing_tables = []

        if not input_aliases:
            # If any of the input_tables are strings, this means they refer
            # to tables that already exist in the database and an alias is not needed
            input_aliases = [
                table if isinstance(table, str) else f"__splink__{ascii_uid(8)}"
                for table in input_tables
            ]

        for table, alias in zip(input_tables, input_aliases):
            if isinstance(table, str):
                # already registered - this should be a table name
                continue
            exists = self.table_exists_in_database(alias)
            # if table exists, and we are not overwriting, we have a problem!
            if exists:
                if not overwrite:
                    existing_tables.append(alias)
                else:
                    self.delete_table_from_database(alias)

        if existing_tables:
            existing_tables_str = ", ".join(existing_tables)
            msg = (
                f"Table(s): {existing_tables_str} already exists in database. "
                "Please remove or rename before retrying"
            )
            raise ValueError(msg)
        for table, alias in zip(input_tables, input_aliases):
            if not isinstance(table, str):
                self._table_registration(table, alias)
                table = alias
            sdf = self.table_to_splink_dataframe(alias, table)
            tables_as_splink_dataframes[alias] = sdf
        return tables_as_splink_dataframes

    @final
    def register_table(self, input, table_name, overwrite=False) -> SplinkDataFrame:
        tables_dict = self.register_multiple_tables(
            [input], [table_name], overwrite=overwrite
        )
        return tables_dict[table_name]

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:
        # returns sql
        # sensible default:
        self.delete_table_from_database(physical_name)
        sql = f"CREATE TABLE {physical_name} AS {sql}"
        return sql

    def _cleanup_for_execute_sql(
        self, table, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        # sensible default:
        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        return output_df

    @abstractmethod
    def _execute_sql_against_backend(self, final_sql: str) -> TablishType:
        pass

    def delete_table_from_database(self, name: str):
        # sensible default:
        drop_sql = f"DROP TABLE IF EXISTS {name}"
        self._execute_sql_against_backend(drop_sql)

    @abstractmethod
    def _table_registration(self, input, table_name: str) -> None:
        """
        Actually register table with backend.

        Overwrite if it already exists.
        """
        pass

    @abstractmethod
    def table_to_splink_dataframe(
        self, templated_name, physical_name
    ) -> SplinkDataFrame:
        pass

    @abstractmethod
    def table_exists_in_database(self, table_name: str) -> bool:
        """
        Check if table_name exists in the backend
        """
        pass

    def process_input_tables(self, input_tables) -> List:
        """
        Process list of input tables from whatever form they arrive in to that suitable
        for linker.
        Default just passes through - backends can specialise if desired
        """
        return input_tables

    # should probably also be responsible for cache
    # TODO: stick this in a cache-api that lives on this

    def remove_splinkdataframe_from_cache(self, splink_dataframe: SplinkDataFrame):
        keys_to_delete = set()
        for key, df in self._intermediate_table_cache.items():
            if df.physical_name == splink_dataframe.physical_name:
                keys_to_delete.add(key)

        for k in keys_to_delete:
            del self._intermediate_table_cache[k]

    def delete_tables_created_by_splink_from_db(self):
        for splink_df in list(self._intermediate_table_cache.values()):
            if splink_df.created_by_splink:
                splink_df.drop_table_from_database_and_remove_from_cache()
