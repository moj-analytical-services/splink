from __future__ import annotations

import hashlib
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union, final

import sqlglot
from pandas import DataFrame as PandasDataFrame

from splink.internals.cache_dict_with_logging import CacheDictWithLogging
from splink.internals.logging_messages import execute_sql_logging_message_info, log_sql
from splink.internals.misc import ascii_uid, ensure_is_list, parse_duration
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame

from .dialects import (
    SplinkDialect,
)
from .exceptions import SplinkException

logger = logging.getLogger(__name__)

# minimal acceptable table types
AcceptableInputTableType = Union[
    str, PandasDataFrame, List[Dict[str, Any]], Dict[str, Any]
]
# a placeholder type. This will depend on the backend subclass - something
# 'tabley' for that backend, such as duckdb.DuckDBPyRelation or spark.DataFrame
TablishType = TypeVar("TablishType")
# general typevar
T = TypeVar("T")


class DatabaseAPI(ABC, Generic[TablishType]):
    sql_dialect: SplinkDialect
    debug_mode: bool = False
    debug_keep_temp_views: bool = False
    """
    DatabaseAPI class handles _all_ interactions with the database
    Anything backend-specific (but not related to SQL dialects) lives here also

    This is intended to be subclassed for specific backends
    """

    def __init__(self) -> None:
        self._intermediate_table_cache: CacheDictWithLogging = CacheDictWithLogging()
        self._cache_uid: str = ascii_uid(8)

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
                    read=self.sql_dialect.sqlglot_dialect,
                ).sql(pretty=True)
                # if sqlglot produces any errors, just report the raw SQL
            except Exception:
                pass

            raise SplinkException(
                f"Error executing the following sql for table "
                f"`{templated_name}`({physical_name}):\n{final_sql}"
                f"\n\nError was: {e}"
            ) from e

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
        # e.g. with compute_df_concat_with_tf()
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
        sql: str,
        output_tablename_templated: str,
        use_cache: bool = True,
    ) -> SplinkDataFrame:
        # differences from _sql_to_splink_dataframe:
        # this _calculates_ physical name, handles debug_mode,
        # and checks cache before querying
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
                table_name_hash,
            )

            self._bind_templated_alias_to_physical(
                output_tablename_templated, table_name_hash
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
        pipeline: CTEPipeline,
        use_cache: bool = True,
    ) -> SplinkDataFrame:
        """
        Execute a given pipeline using input_dataframes as seeds if provided.
        self.debug_mode controls whether this is CTE or individual tables.
        pipeline is set to spent after execution ensuring it cannot be
        acidentally reused
        """

        if not self.debug_mode:
            sql_gen = pipeline.generate_cte_pipeline_sql()
            output_tablename_templated = pipeline.output_table_name

            return self.sql_to_splink_dataframe_checking_cache(
                sql_gen,
                output_tablename_templated,
                use_cache,
            )

        created_views: list[str] = []
        created_physicals: list[str] = []
        pipeline_completed = False
        splink_dataframe: Optional[SplinkDataFrame] = None

        try:
            for cte in pipeline.ctes_pipeline():
                start_time = time.time()
                output_tablename = cte.output_table_name
                sql = cte.sql
                print("------")  # noqa: T201
                print(  # noqa: T201
                    f"--------Creating table: {output_tablename}--------"
                )

                splink_dataframe = self.sql_to_splink_dataframe_checking_cache(
                    sql,
                    output_tablename,
                    use_cache=False,
                )
                created_views.append(output_tablename)
                created_physicals.append(splink_dataframe.physical_name)

                run_time = parse_duration(time.time() - start_time)
                print(f"Step ran in: {run_time}")  # noqa: T201

            pipeline_completed = True
        finally:
            self._cleanup_debug_overlays(
                created_views,
                created_physicals,
                pipeline_completed,
            )
            # don't want to cache anything in debug mode
            self._intermediate_table_cache.invalidate_cache()

        if splink_dataframe is None:
            raise SplinkException("Debug pipeline execution produced no output tables.")

        return splink_dataframe

    @final
    def register_multiple_tables(
        self,
        input_tables: Sequence[AcceptableInputTableType],
        input_aliases: Optional[List[str]] = None,
        overwrite: bool = False,
    ) -> Dict[str, SplinkDataFrame]:
        input_tables = self.process_input_tables(input_tables)

        tables_as_splink_dataframes = {}
        existing_tables = []

        if not input_aliases:
            input_aliases = [f"__splink__{ascii_uid(8)}" for table in input_tables]

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
    def register_table(
        self,
        input_table: AcceptableInputTableType,
        table_name: str,
        overwrite: bool = False,
    ) -> SplinkDataFrame:
        tables_dict = self.register_multiple_tables(
            [input_table], [table_name], overwrite=overwrite
        )
        return tables_dict[table_name]

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:
        # returns sql
        # sensible default:
        self.delete_table_from_database(physical_name)
        sql = f"CREATE TABLE {physical_name} AS {sql}"
        return sql

    def _cleanup_for_execute_sql(
        self, table: TablishType, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        # sensible default:
        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        return output_df

    @abstractmethod
    def _execute_sql_against_backend(self, final_sql: str) -> TablishType:
        pass

    def delete_table_from_database(self, name: str) -> None:
        # sensible default:
        drop_sql = f"DROP TABLE IF EXISTS {name}"
        self._execute_sql_against_backend(drop_sql)

    @abstractmethod
    def _table_registration(
        self, input: AcceptableInputTableType, table_name: str
    ) -> None:
        """
        Actually register table with backend.

        Overwrite if it already exists.
        """
        pass

    @abstractmethod
    def table_to_splink_dataframe(
        self, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        pass

    @abstractmethod
    def table_exists_in_database(self, table_name: str) -> bool:
        """
        Check if table_name exists in the backend
        """
        pass

    def process_input_tables(
        self, input_tables: Sequence[AcceptableInputTableType]
    ) -> Sequence[AcceptableInputTableType]:
        """
        Process list of input tables from whatever form they arrive in to that suitable
        for linker.
        Default just passes through - backends can specialise if desired
        """
        input_tables = ensure_is_list(input_tables)
        return input_tables

    def remove_splinkdataframe_from_cache(
        self, splink_dataframe: SplinkDataFrame
    ) -> None:
        keys_to_delete = set()
        for key, df in self._intermediate_table_cache.items():
            if df.physical_name == splink_dataframe.physical_name:
                keys_to_delete.add(key)

        for k in keys_to_delete:
            del self._intermediate_table_cache[k]

    def delete_tables_created_by_splink_from_db(self):
        # Accounts for names in cache with key which are templated names
        keys = list(self._intermediate_table_cache.keys())

        for key in keys:
            if key in self._intermediate_table_cache:
                splink_df = self._intermediate_table_cache[key]
            else:
                continue
            if (
                key in self._intermediate_table_cache
                and splink_df.created_by_splink
                and key == splink_df.physical_name
            ):
                splink_df.drop_table_from_database_and_remove_from_cache()

    def _bind_templated_alias_to_physical(self, templated: str, physical: str) -> None:
        """Expose the physical table via a backend-specific temp view."""
        self._create_or_replace_temp_view(templated, physical)

    def _unbind_templated_alias_from_physical(self, templated: str) -> None:
        self._drop_temp_view_if_exists(templated)

    def _create_or_replace_temp_view(self, name: str, physical: str) -> None:
        self._execute_sql_against_backend(
            f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM {physical}"
        )

    def _drop_temp_view_if_exists(self, name: str) -> None:
        self._execute_sql_against_backend(f"DROP VIEW IF EXISTS {name}")

    def _cleanup_debug_overlays(
        self,
        templated_names: Sequence[str],
        physical_names: Sequence[str],
        pipeline_completed: bool,
    ) -> None:
        if self.debug_keep_temp_views:
            return

        if not templated_names:
            return

        for templated_name in reversed(templated_names):
            self._unbind_templated_alias_from_physical(templated_name)

        if not physical_names:
            return

        keep_upto = len(physical_names)
        if pipeline_completed:
            # Always retain the final physical table - it's returned to the caller.
            keep_upto -= 1

        for physical_name in physical_names[:keep_upto]:
            try:
                self.delete_table_from_database(physical_name)
            except Exception as exc:  # pragma: no cover - best effort cleanup
                logger.debug(
                    "Unable to drop intermediate debug table %s: %s",
                    physical_name,
                    exc,
                )


DatabaseAPISubClass = DatabaseAPI[Any]
