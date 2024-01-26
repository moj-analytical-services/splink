import logging
import math
import os
import re
import sqlite3
from abc import ABC, abstractmethod
from itertools import compress
from tempfile import TemporaryDirectory
from typing import Dict, Generic, List, TypeVar, Union, final

import duckdb
import pandas as pd
import sqlglot
from numpy import nan
from pyspark.sql.dataframe import DataFrame as spark_df
from pyspark.sql.utils import AnalysisException
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .cache_dict_with_logging import CacheDictWithLogging
from .databricks.enable_splink import enable_splink
from .dialects import (
    DuckDBDialect,
    PostgresDialect,
    SparkDialect,
    SplinkDialect,
    SQLiteDialect,
)
from .duckdb.duckdb_helpers.duckdb_helpers import (
    create_temporary_duckdb_connection,
    duckdb_load_from_file,
    validate_duckdb_connection,
)
from .duckdb.linker import DuckDBDataFrame
from .exceptions import SplinkException
from .logging_messages import execute_sql_logging_message_info, log_sql
from .misc import ensure_is_list, major_minor_version_greater_equal_than
from .postgres.linker import PostgresDataFrame
from .spark.jar_location import get_scala_udfs
from .spark.linker import SparkDataFrame
from .splink_dataframe import SplinkDataFrame
from .sqlite.linker import SQLiteDataFrame

logger = logging.getLogger(__name__)


# a placeholder type. This will depend on the backend subclass - something
# 'tabley' for that backend, such as duckdb.DuckDBPyRelation or spark.DataFrame
TablishType = TypeVar("TablishType")


class DatabaseAPI(ABC, Generic[TablishType]):
    sql_dialect: SplinkDialect
    """
    DatabaseAPI class handles _all_ interactions with the database
    Anything backend-specific (but not related to SQL dialects) lives here also

    This is intended to be subclassed for specific backends
    """

    def __init__(self) -> None:
        self._intermediate_table_cache: CacheDictWithLogging = CacheDictWithLogging()

    @final
    def log_and_run_sql_execution(
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
            return self._run_sql_execution(final_sql)
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
    def execute_sql_against_backend(
        self, sql: str, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        """
        Create a table in the backend using some given sql

        Table will have physical_name in the backend.

        Returns a SplinkDataFrame which also uses templated_name
        """
        sql = self._setup_for_execute_sql(sql, physical_name)
        spark_df = self.log_and_run_sql_execution(sql, templated_name, physical_name)
        output_df = self._cleanup_for_execute_sql(
            spark_df, templated_name, physical_name
        )
        return output_df

    @final
    def register_multiple_tables(
        self, input_tables, input_aliases, overwrite=False
    ) -> Dict[str, SplinkDataFrame]:
        tables_as_splink_dataframes = {}
        existing_tables = []
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
                    self._delete_table_from_database(alias)

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
        self._delete_table_from_database(physical_name)
        sql = f"CREATE TABLE {physical_name} AS {sql}"
        return sql

    def _cleanup_for_execute_sql(
        self, table, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        # sensible default:
        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        return output_df

    @abstractmethod
    def _run_sql_execution(self, final_sql: str) -> TablishType:
        pass

    def _delete_table_from_database(self, name: str):
        # sensible default:
        drop_sql = f"DROP TABLE IF EXISTS {name}"
        self._run_sql_execution(drop_sql)

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

    def _remove_splinkdataframe_from_cache(self, splink_dataframe: SplinkDataFrame):
        keys_to_delete = set()
        for key, df in self._intermediate_table_cache.items():
            if df.physical_name == splink_dataframe.physical_name:
                keys_to_delete.add(key)

        for k in keys_to_delete:
            del self._intermediate_table_cache[k]


# alias for brevity:
ddb_con = duckdb.DuckDBPyConnection
sql_con = sqlite3.Connection


class DuckDBAPI(DatabaseAPI):
    sql_dialect = DuckDBDialect()

    def __init__(
        self,
        connection: Union[str, ddb_con] = ":memory:",
        output_schema: str = None,
    ):
        super().__init__()
        validate_duckdb_connection(connection, logger)

        if isinstance(connection, str):
            con_lower = connection.lower()
        if isinstance(connection, ddb_con):
            con = connection
        elif con_lower == ":memory:":
            con = duckdb.connect(database=connection)
        elif con_lower == ":temporary:":
            con = create_temporary_duckdb_connection(self)
        else:
            con = duckdb.connect(database=connection)

        self._con = con

        if output_schema:
            self._con.execute(
                f"""
                    CREATE SCHEMA IF NOT EXISTS {output_schema};
                    SET schema '{output_schema}';
                """
            )

    def _table_registration(self, input, table_name) -> None:
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Registration errors will automatically
        # occur if an invalid data type is passed as an argument
        self._con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM input")

    def table_to_splink_dataframe(
        self, templated_name, physical_name
    ) -> DuckDBDataFrame:
        return DuckDBDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"

        # From duckdb 0.5.0, duckdb will raise a CatalogException
        # which does not exist in 0.4.0 or before

        # TODO: probably we can drop this compat now?
        try:
            from duckdb import CatalogException

            error = (RuntimeError, CatalogException)
        except ImportError:
            error = RuntimeError

        try:
            self._con.execute(sql)
        except error:
            return False
        return True

    def load_from_file(self, file_path: str):
        return duckdb_load_from_file(file_path)

    def _run_sql_execution(self, final_sql: str) -> duckdb.DuckDBPyRelation:
        return self._con.sql(final_sql)

    @property
    def accepted_df_dtypes(self):
        accepted_df_dtypes = [pd.DataFrame]
        try:
            # If pyarrow is installed, add to the accepted list
            import pyarrow as pa

            accepted_df_dtypes.append(pa.lib.Table)
        except ImportError:
            pass
        return accepted_df_dtypes

    def process_input_tables(self, input_tables):
        return [
            self.load_from_file(t) if isinstance(t, str) else t for t in input_tables
        ]

    # special methods for use:

    def export_to_duckdb_file(self, output_path, delete_intermediate_tables=False):
        """
        https://stackoverflow.com/questions/66027598/how-to-vacuum-reduce-file-size-on-duckdb
        """
        if delete_intermediate_tables:
            self._delete_tables_created_by_splink_from_db()
        with TemporaryDirectory() as tmpdir:
            self._con.execute(f"EXPORT DATABASE '{tmpdir}' (FORMAT PARQUET);")
            new_con = duckdb.connect(database=output_path)
            new_con.execute(f"IMPORT DATABASE '{tmpdir}';")
            new_con.close()


class SparkAPI(DatabaseAPI):
    sql_dialect = SparkDialect()

    def __init__(
        self,
        break_lineage_method=None,
        spark=None,
        catalog=None,
        database=None,
        # TODO: what to do about repartitions:
        repartition_after_blocking=False,
        num_partitions_on_repartition=None,
        register_udfs_automatically=True,
    ):
        super().__init__()
        # TODO: revise logic as necessary!
        self.break_lineage_method = break_lineage_method

        # these properties will be needed whenever spark is _actually_ set up
        self.repartition_after_blocking = repartition_after_blocking
        self.num_partitions_on_repartition = num_partitions_on_repartition
        self.catalog = catalog
        self.database = database
        self.register_udfs_automatically = register_udfs_automatically

        # TODO: hmmm breaking this flow. Lazy spark ??
        # self._get_spark_from_input_tables_if_not_provided(spark, input_tables)
        self.spark = spark

        # TODO: also need to think about where these live:
        # self._drop_splink_cached_tables()
        # self._check_ansi_enabled_if_converting_dates()

        # TODO: (ideally) set things up so databricks can inherit from this
        self.in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
        if self.in_databricks:
            enable_splink(spark)

        self._set_default_break_lineage_method()

    def _table_registration(self, input, table_name) -> None:
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        if isinstance(input, pd.DataFrame):
            input = self._clean_pandas_df(input)
            input = self.spark.createDataFrame(input)

        input.createOrReplaceTempView(table_name)

    def table_to_splink_dataframe(
        self, templated_name, physical_name
    ) -> SparkDataFrame:
        return SparkDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        query_result = self.spark.sql(
            f"show tables from {self.splink_data_store} like '{table_name}'"
        ).collect()
        if len(query_result) > 1:
            # this clause accounts for temp tables which can have the same name as
            # persistent table without issue
            if (
                len({x.tableName for x in query_result}) == 1
            ) and (  # table names are the same
                len({x.isTemporary for x in query_result}) == 2
            ):  # isTemporary is boolean
                return True
            else:
                raise ValueError(
                    f"Table name {table_name} not unique. Does it contain a wild card?"
                )
        elif len(query_result) == 1:
            return True
        elif len(query_result) == 0:
            return False

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:
        sql = sqlglot.transpile(sql, read="spark", write="customspark", pretty=True)[0]
        return sql

    def _cleanup_for_execute_sql(
        self, table: spark_df, templated_name: str, physical_name: str
    ):
        spark_df = self._break_lineage_and_repartition(
            table, templated_name, physical_name
        )

        # After blocking, want to repartition
        # if templated
        spark_df.createOrReplaceTempView(physical_name)

        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        return output_df

    def _run_sql_execution(self, final_sql: str) -> spark_df:
        return self.spark.sql(final_sql)

    def _delete_table_from_database(self, name):
        self.spark.sql(f"drop table {name}")

    @property
    def accepted_df_dtypes(self):
        return [pd.DataFrame, spark_df]

    def process_input_tables(self, input_tables):
        # if we don't have a spark instance yet, grab it from provided tables
        if self.spark is None:
            self._get_spark_from_input_tables(input_tables)
        return input_tables

    # special methods:
    @property
    def spark(self):
        return self._spark

    @spark.setter
    def spark(self, spark):
        self._spark = spark
        if spark is None:
            return
        # if we have a proper spark instance, then set it up!
        self.set_default_num_partitions_on_repartition_if_missing()
        self._set_catalog_and_database_if_not_provided(self.catalog, self.database)
        if self.register_udfs_automatically:
            self._register_udfs_from_jar()

    def _get_spark_from_input_tables(self, input_tables):
        spark_inputs = [isinstance(d, spark_df) for d in input_tables]
        if any(spark_inputs):
            for t in list(compress(input_tables, spark_inputs)):
                # t.sparkSession can be used only from spark 3.3.0 onwards
                self.spark = t.sql_ctx.sparkSession
                break

        if self.spark is None:
            raise ValueError(
                "If input_table_or_tables are strings or pandas dataframes rather than "
                "Spark dataframes, you must pass in the spark session using the spark="
                " argument when you initialise SparkAPI."
            )

    def _clean_pandas_df(self, df):
        return df.fillna(nan).replace([nan, pd.NA], [None, None])

    def _set_catalog_and_database_if_not_provided(self, catalog, database):
        # spark.catalog.currentCatalog() is not available in versions of spark before
        # 3.4.0. In Spark versions less that 3.4.0 we will require explicit catalog
        # setting, but will revert to default in Spark versions greater than 3.4.0
        threshold = "3.4.0"
        self.catalog = catalog
        if (
            major_minor_version_greater_equal_than(self.spark.version, threshold)
            and not self.catalog
        ):
            # set the catalog and database of where to write output tables
            self.catalog = (
                catalog if catalog is not None else self.spark.catalog.currentCatalog()
            )
        self.database = (
            database if database is not None else self.spark.catalog.currentDatabase()
        )

        # this defines the catalog.database location where splink's data outputs will
        # be stored. The filter will remove none, so if catalog is not provided and
        # spark version is < 3.3.0 we will use the default catalog.
        self.splink_data_store = ".".join(
            [f"`{x}`" for x in [self.catalog, self.database] if x is not None]
        )

    def _register_udfs_from_jar(self):
        # TODO: this should check if these are already registered and skip if so
        # to cut down on warnings

        # Grab all available udfs and required info to register them
        udfs_register = get_scala_udfs()

        try:
            # Register our scala functions. Note that this will only work if the jar has
            # been registered by the user
            for udf in udfs_register:
                self.spark.udf.registerJavaFunction(*udf)
        except AnalysisException as e:
            logger.warning(
                "Unable to load custom Spark SQL functions such as jaro_winkler from "
                "the jar that's provided with Splink.\n"
                "You need to ensure the Splink jar is registered.\n"
                "See https://moj-analytical-services.github.io/splink/demos/example_simple_pyspark.html "  # NOQA: E501
                "for an example.\n"
                "You will not be able to use these functions in your linkage.\n"
                "You can find the location of the jar by calling the following function"
                ":\nfrom splink.spark.jar_location import similarity_jar_location"
                "\n\nFull error:\n"
                f"{e}"
            )

    def _get_checkpoint_dir_path(self, spark_df):
        # https://github.com/apache/spark/blob/301a13963808d1ad44be5cacf0a20f65b853d5a2/python/pyspark/context.py#L1323 # noqa E501
        # getCheckpointDir method exists only in Spark 3.1+, use implementation
        # from above link
        if not self.spark._jsc.sc().getCheckpointDir().isEmpty():
            return self.spark._jsc.sc().getCheckpointDir().get()
        else:
            # Raise checkpointing error
            spark_df.limit(1).checkpoint()

    def set_default_num_partitions_on_repartition_if_missing(self):
        if self.num_partitions_on_repartition is None:
            parallelism_value = 200
            try:
                parallelism_value = self.spark.conf.get("spark.default.parallelism")
                parallelism_value = int(parallelism_value)
            except Exception:
                pass

            # Prefer spark.sql.shuffle.partitions if set
            try:
                parallelism_value = self.spark.conf.get("spark.sql.shuffle.partitions")
                parallelism_value = int(parallelism_value)
            except Exception:
                pass

            self.num_partitions_on_repartition = math.ceil(parallelism_value / 2)

    # TODO: this repartition jazz knows too much about the linker
    def _repartition_if_needed(self, spark_df, templated_name):
        # Repartitioning has two effects:
        # 1. When we persist out results to disk, it results in a predictable
        #    number of output files.  Some splink operations result in a very large
        #    number of output files, so this reduces the number of files and therefore
        #    avoids slow reads and writes
        # 2. When we repartition, it results in a more evenly distributed workload
        #    across the cluster, which is useful for large datasets.

        names_to_repartition = [
            r"__splink__df_comparison_vectors",
            r"__splink__df_blocked",
            r"__splink__df_neighbours",
            r"__splink__df_representatives",
            r"__splink__df_concat_with_tf_sample",
            r"__splink__df_concat_with_tf",
            r"__splink__df_predict",
        ]

        num_partitions = self.num_partitions_on_repartition

        # TODO: why regex not == ?
        if re.fullmatch(r"__splink__df_predict", templated_name):
            num_partitions = math.ceil(self.num_partitions_on_repartition)

        if re.fullmatch(r"__splink__df_representatives", templated_name):
            num_partitions = math.ceil(self.num_partitions_on_repartition / 6)

        if re.fullmatch(r"__splink__df_neighbours", templated_name):
            num_partitions = math.ceil(self.num_partitions_on_repartition / 4)

        if re.fullmatch(r"__splink__df_concat_with_tf_sample", templated_name):
            num_partitions = math.ceil(self.num_partitions_on_repartition / 4)

        if re.fullmatch(r"__splink__df_concat_with_tf", templated_name):
            num_partitions = math.ceil(self.num_partitions_on_repartition / 4)

        if re.fullmatch(r"|".join(names_to_repartition), templated_name):
            spark_df = spark_df.repartition(num_partitions)

        return spark_df

    def _break_lineage_and_repartition(self, spark_df, templated_name, physical_name):
        spark_df = self._repartition_if_needed(spark_df, templated_name)

        regex_to_persist = [
            r"__splink__df_comparison_vectors",
            r"__splink__df_concat_with_tf",
            r"__splink__df_predict",
            r"__splink__df_tf_.+",
            r"__splink__df_representatives.*",
            r"__splink__df_neighbours",
            r"__splink__df_connected_components_df",
        ]

        if re.fullmatch(r"|".join(regex_to_persist), templated_name):
            if self.break_lineage_method == "persist":
                spark_df = spark_df.persist()
                logger.debug(f"persisted {templated_name}")
            elif self.break_lineage_method == "checkpoint":
                spark_df = spark_df.checkpoint()
                logger.debug(f"Checkpointed {templated_name}")
            elif self.break_lineage_method == "parquet":
                checkpoint_dir = self._get_checkpoint_dir_path(spark_df)
                write_path = os.path.join(checkpoint_dir, physical_name)
                spark_df.write.mode("overwrite").parquet(write_path)
                spark_df = self.spark.read.parquet(write_path)
                logger.debug(f"Wrote {templated_name} to parquet")
            elif self.break_lineage_method == "delta_lake_files":
                checkpoint_dir = self._get_checkpoint_dir_path(spark_df)
                write_path = os.path.join(checkpoint_dir, physical_name)
                spark_df.write.mode("overwrite").format("delta").save()
                spark_df = self.spark.read.format("delta").load(write_path)
                logger.debug(f"Wrote {templated_name} to Delta files at {write_path}")
            elif self.break_lineage_method == "delta_lake_table":
                write_path = f"{self.splink_data_store}.{physical_name}"
                spark_df.write.mode("overwrite").saveAsTable(write_path)
                spark_df = self.spark.table(write_path)
                logger.debug(
                    f"Wrote {templated_name} to Delta Table at "
                    f"{self.splink_data_store}.{physical_name}"
                )
            else:
                raise ValueError(
                    f"Unknown break_lineage_method: {self.break_lineage_method}"
                )
        return spark_df

    def _set_default_break_lineage_method(self):
        # check to see if running in databricks and use delta lake tables
        # as break lineage method if nothing else specified.

        if self.in_databricks and not self.break_lineage_method:
            self.break_lineage_method = "delta_lake_table"
            logger.info(
                "Intermediate results will be written as Delta Lake tables at "
                f"{self.splink_data_store}."
            )

        # set non-databricks environment default method as parquet in case nothing else
        # specified.
        elif not self.break_lineage_method:
            self.break_lineage_method = "parquet"


class SQLiteAPI(DatabaseAPI):
    sql_dialect = SQLiteDialect()

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def _register_udfs(self, register_udfs: bool):
        self.con.create_function("log2", 1, math.log2)
        self.con.create_function("pow", 2, pow)
        self.con.create_function("power", 2, pow)

        if register_udfs:
            try:
                from rapidfuzz.distance.DamerauLevenshtein import distance as dam_lev
                from rapidfuzz.distance.Jaro import distance as jaro
                from rapidfuzz.distance.JaroWinkler import distance as jaro_winkler
                from rapidfuzz.distance.Levenshtein import distance as levenshtein
            except ModuleNotFoundError as e:
                raise SplinkException(
                    "To use fuzzy string-matching udfs in SQLite you must install "
                    "the python package 'rapidfuzz'.  "
                    "If you do not wish to do so, and do not need to use any "
                    "fuzzy string-matching comparisons, you can use the "
                    "linker argument `register_udfs=False`.\n"
                    "See https://moj-analytical-services.github.io/splink/"
                    "topic_guides/backends.html#sqlite for more information"
                ) from e

        def wrap_func_with_str(func):
            def wrapped_func(str_l, str_r):
                return func(str(str_l), str(str_r))

            return wrapped_func

        funcs_to_register = {
            "levenshtein": levenshtein,
            "damerau_levenshtein": dam_lev,
            "jaro_winkler": jaro_winkler,
            "jaro": jaro,
        }

        for sql_name, func in funcs_to_register.items():
            self.con.create_function(sql_name, 2, wrap_func_with_str(func))

    def __init__(
        self, connection: Union[str, sql_con] = ":memory:", register_udfs=True
    ):
        super().__init__()

        if isinstance(connection, str):
            connection = sqlite3.connect(connection)
        self.con = connection
        self.con.row_factory = self.dict_factory
        self._register_udfs(register_udfs)

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Will error if an invalid data type is passed
        input.to_sql(
            table_name,
            self.con,
            index=False,
            if_exists="replace",
        )

    def table_to_splink_dataframe(self, templated_name, physical_name):
        return SQLiteDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"

        rec = self.con.execute(sql).fetchone()
        if not rec:
            return False
        else:
            return True

    def _run_sql_execution(self, final_sql: str) -> sqlite3.Cursor:
        return self.con.execute(final_sql)


class PostgresAPI(DatabaseAPI):
    sql_dialect = PostgresDialect()

    def __init__(
        self,
        engine: Engine,
        schema: str = "splink",
        other_schemas_to_search: Union[str, List[str]] = [],
    ):
        super().__init__()
        if not isinstance(engine, Engine):
            raise ValueError(
                "You must supply a sqlalchemy engine to create a PostgresAPI."
            )

        self._engine = engine
        self._db_schema = schema
        self._create_splink_schema(other_schemas_to_search)

        self._register_custom_functions()
        self._register_extensions()

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Will error if an invalid data type is passed
        input.to_sql(
            table_name,
            con=self._engine,
            index=False,
            if_exists="replace",
            schema=self._db_schema,
        )

    def table_to_splink_dataframe(self, templated_name, physical_name):
        return PostgresDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = '{table_name}';
        """

        rec = self._run_sql_execution(sql).fetchall()
        return len(rec) > 0

    def _run_sql_execution(
        self, final_sql: str, templated_name: str = None, physical_name: str = None
    ):
        with self._engine.connect() as con:
            res = con.execute(text(final_sql))
        return res

    # postgres udf registrations:
    def _create_log2_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION log2(n float8)
        RETURNS float8 AS $$
        SELECT log(2.0, n::numeric)::float8;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

    def _extend_round_function(self):
        # extension of round to double
        sql = """
        CREATE OR REPLACE FUNCTION round(n float8, dp integer)
        RETURNS numeric AS $$
        SELECT round(n::numeric, dp);
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

    def _create_try_cast_date_function(self):
        # postgres to_date will give an error if the date can't be parsed
        # to be consistent with other backends we instead create a version
        # which instead returns NULL, allowing us more flexibility
        sql = """
        CREATE OR REPLACE FUNCTION try_cast_date(date_string text, format text)
        RETURNS date AS $func$
        BEGIN
            BEGIN
                RETURN to_date(date_string, format);
            EXCEPTION WHEN OTHERS THEN
                RETURN NULL;
            END;
        END
        $func$ LANGUAGE plpgsql IMMUTABLE;
        """
        self._run_sql_execution(sql)

    def _create_datediff_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION datediff(x date, y date)
        RETURNS integer AS $$
        SELECT x - y;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

        sql_cast = """
        CREATE OR REPLACE FUNCTION datediff(x {dateish_type}, y {dateish_type})
        RETURNS integer AS $$
        SELECT datediff(DATE(x), DATE(y));
        $$ LANGUAGE SQL IMMUTABLE;
        """
        for dateish_type in ("timestamp", "timestamp with time zone"):
            self._run_sql_execution(sql_cast.format(dateish_type=dateish_type))

    def _create_months_between_function(self):
        # number of average-length (per year) months between two dates
        # logic could be improved/made consistent with other backends
        # but this is reasonable for now
        # 30.4375 days
        ave_length_month = 365.25 / 12
        sql = f"""
        CREATE OR REPLACE FUNCTION ave_months_between(x date, y date)
        RETURNS float8 AS $$
        SELECT (datediff(x, y)/{ave_length_month})::float8;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

        sql_cast = """
        CREATE OR REPLACE FUNCTION ave_months_between(
            x {dateish_type}, y {dateish_type}
        )
        RETURNS integer AS $$
        SELECT (ave_months_between(DATE(x), DATE(y)))::int;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        for dateish_type in ("timestamp", "timestamp with time zone"):
            self._run_sql_execution(sql_cast.format(dateish_type=dateish_type))

    def _create_array_intersect_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION array_intersect(x anyarray, y anyarray)
        RETURNS anyarray AS $$
        SELECT ARRAY( SELECT DISTINCT * FROM UNNEST(x) WHERE UNNEST = ANY(y) )
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

    def _register_custom_functions(self):
        # if people have issues with permissions we can allow these to be optional
        # need for predict_from_comparison_vectors_sql (could adjust)
        self._create_log2_function()
        # need for date-casting
        self._create_try_cast_date_function()
        # need for datediff levels
        self._create_datediff_function()
        self._create_months_between_function()
        # need for array_intersect levels
        self._create_array_intersect_function()
        # extension of round to handle doubles - used in unlinkables
        self._extend_round_function()

    def _register_extensions(self):
        sql = """
        CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
        """
        self._run_sql_execution(sql)

    def _create_splink_schema(self, other_schemas_to_search):
        other_schemas_to_search = ensure_is_list(other_schemas_to_search)
        # always search _db_schema first, and public last
        schemas_to_search = [self._db_schema] + other_schemas_to_search + ["public"]
        search_path = ",".join(schemas_to_search)
        sql = f"""
        CREATE SCHEMA IF NOT EXISTS {self._db_schema};
        SET search_path TO {search_path};
        """
        self._run_sql_execution(sql)
