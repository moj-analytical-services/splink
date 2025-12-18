from __future__ import annotations

import hashlib
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
    final,
)

import pyarrow as pa
import sqlglot

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

BaseAcceptableInputTableType = Union[
    str,
    List[Dict[str, Any]],
    Dict[str, Any],
    pa.Table,
]

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame

    AcceptableInputTableType = Union[BaseAcceptableInputTableType, PandasDataFrame]
else:
    AcceptableInputTableType = BaseAcceptableInputTableType

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
        self._id: str = ascii_uid(8)
        self._created_tables: set[str] = set()
        self._input_table_counter: int = 0
        self._registered_source_dataset_names: set[str] = set()

    @property
    @final
    def id(self) -> str:
        """Useful for debugging when multiple database API instances exist."""
        return self._id

    def _new_input_table_name(self) -> str:
        name = f"__splink__input_table_{self._input_table_counter}"
        self._input_table_counter += 1
        return name

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
        self._created_tables.add(physical_name)
        return output_df

    @final
    def _execute_sql(
        self,
        sql: str,
        output_tablename_templated: str,
    ) -> SplinkDataFrame:
        """Execute SQL and return a SplinkDataFrame.

        This is a low-level method that:
        1. Generates a unique physical table name (using hash of SQL)
        2. Executes the SQL to create the table
        3. Returns a SplinkDataFrame wrapping the result

        For most use cases, prefer `sql_pipeline_to_splink_dataframe()` which
        takes a CTEPipeline.
        """
        to_hash = (sql + self._cache_uid).encode("utf-8")
        hash = hashlib.sha256(to_hash).hexdigest()[:9]
        table_name_hash = f"{output_tablename_templated}_{hash}"

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

        return splink_dataframe

    def sql_pipeline_to_splink_dataframe(
        self,
        pipeline: CTEPipeline,
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

            return self._execute_sql(
                sql_gen,
                output_tablename_templated,
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

                splink_dataframe = self._execute_sql(
                    sql,
                    output_tablename,
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

    # See https://github.com/moj-analytical-services/splink/pull/2863#issue-3738534958
    # for notes on this code
    def register(
        self,
        table: AcceptableInputTableType | str,
        source_dataset_name: Optional[str] = None,
    ) -> SplinkDataFrame:
        if source_dataset_name is not None:
            if source_dataset_name in self._registered_source_dataset_names:
                raise ValueError(
                    f"A table has already been registered with "
                    f"source_dataset_name='{source_dataset_name}'. "
                    f"Each registered table must have a unique source_dataset_name."
                )
            self._registered_source_dataset_names.add(source_dataset_name)

        templated_name = source_dataset_name or self._new_input_table_name()

        # String inputs represent already-registered physical tables.
        # If `source_dataset_name` is not provided, we still generate a fresh internal
        # templated name so that the same physical table can be used multiple times as
        # distinct inputs (e.g. linking a table to itself).
        if isinstance(table, str):
            physical_name = table
            sdf = self.table_to_splink_dataframe(templated_name, physical_name)
        else:
            # Allow overwrite of table only if Splink is assigning the name
            # i.e. allow overwrites of tables of the form __splink__input_table_n
            overwrite = source_dataset_name is None
            sdf = self._create_backend_table(table, templated_name, overwrite=overwrite)

        sdf.source_dataset_name = templated_name
        return sdf

    @final
    def _create_backend_table(
        self,
        input_table: AcceptableInputTableType,
        templated_name: str,
        overwrite: bool = False,
    ) -> SplinkDataFrame:
        # If input is a string, it's already a registered table name in the database.
        # Just create a SplinkDataFrame wrapper pointing to it using the templated
        # name
        if isinstance(input_table, str):
            return self.table_to_splink_dataframe(templated_name, input_table)

        exists = self.table_exists_in_database(templated_name)
        if exists:
            if not overwrite:
                raise ValueError(
                    f"Table '{templated_name}' already exists in database. "
                    "Please remove or rename before retrying"
                )
            else:
                self.delete_table_from_database(templated_name)

        self._table_registration(input_table, templated_name)
        return self.table_to_splink_dataframe(templated_name, templated_name)

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
        # User-registered tables are never added to _created_tables,
        # so don't need to worry about accidentally deleting them.
        for physical_name in list(self._created_tables):
            self.delete_table_from_database(physical_name)
            self._created_tables.discard(physical_name)

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
