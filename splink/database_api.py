import logging
import math
import os
import re
from tempfile import TemporaryDirectory

import duckdb
import pandas as pd
import sqlglot
from numpy import nan
from pyspark.sql.dataframe import DataFrame as spark_df
from pyspark.sql.utils import AnalysisException

from .cache_dict_with_logging import CacheDictWithLogging
from .databricks.enable_splink import enable_splink
from .dialects import DuckDBDialect, SparkDialect
from .duckdb.duckdb_helpers.duckdb_helpers import (
    create_temporary_duckdb_connection,
    duckdb_load_from_file,
    validate_duckdb_connection,
)
from .duckdb.linker import DuckDBDataFrame
from .exceptions import SplinkException
from .logging_messages import execute_sql_logging_message_info, log_sql
from .misc import major_minor_version_greater_equal_than
from .spark.jar_location import get_scala_udfs
from .spark.linker import SparkDataFrame
from .splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


class DatabaseAPI:
    def __init__(self):
        self._intermediate_table_cache: dict = CacheDictWithLogging()

    def _log_and_run_sql_execution(
        self, final_sql: str, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        """Log the sql, then call _run_sql_execution(), wrapping any errors"""
        logger.debug(execute_sql_logging_message_info(templated_name, physical_name))
        logger.log(5, log_sql(final_sql))
        try:
            return self._run_sql_execution(final_sql, templated_name, physical_name)
        except Exception as e:
            # Parse our SQL through sqlglot to pretty print
            try:
                final_sql = sqlglot.parse_one(
                    final_sql,
                    read=self._sql_dialect,
                ).sql(pretty=True)
                # if sqlglot produces any errors, just report the raw SQL
            except Exception:
                pass

            raise SplinkException(
                f"Error executing the following sql for table "
                f"`{templated_name}`({physical_name}):\n{final_sql}"
                f"\n\nError was: {e}"
            ) from e

    def execute_sql_against_backend(
        self, sql: str, templated_name: str, physical_name: str
    ):
        sql = self._setup_for_execute_sql(sql, physical_name)
        spark_df = self._log_and_run_sql_execution(sql, templated_name, physical_name)
        output_df = self._cleanup_for_execute_sql(
            spark_df, templated_name, physical_name
        )
        return output_df

    def register_table(self, input, table_name, overwrite=False):
        # If the user has provided a table name, return it as a SplinkDataframe
        if isinstance(input, str):
            return self.table_to_splink_dataframe(table_name, input)

        # Check if table name is already in use
        exists = self.table_exists_in_database(table_name)
        if exists:
            if not overwrite:
                raise ValueError(
                    f"Table '{table_name}' already exists in database. "
                    "Please use the 'overwrite' argument if you wish to overwrite"
                )
            else:
                # TODO: agnosticise
                self._con.unregister(table_name)

        self._table_registration(input, table_name)
        return self.table_to_splink_dataframe(table_name, table_name)

    def process_input_tables(self, input_tables):
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


class DuckDBAPI(DatabaseAPI):
    sql_dialect = DuckDBDialect()

    def __init__(
        self,
        connection: str = ":memory:",
        output_schema: str = None,
    ):
        super().__init__()
        validate_duckdb_connection(connection, logger)

        if isinstance(connection, str):
            con_lower = connection.lower()
        if isinstance(connection, duckdb.DuckDBPyConnection):
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
        self._con.register(table_name, input)

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

    def _setup_for_execute_sql(self, sql, physical_name):
        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self._delete_table_from_database(physical_name)

        sql = f"""
        CREATE TABLE {physical_name}
        AS
        ({sql})
        """
        return sql

    def _cleanup_for_execute_sql(self, table, templated_name, physical_name):

        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        return output_df

    def _run_sql_execution(self, final_sql, templated_name, physical_name):
        self._con.execute(final_sql)

    def _delete_table_from_database(self, name):
        drop_sql = f"""
        DROP TABLE IF EXISTS {name}"""
        self._con.execute(drop_sql)

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

        self.repartition_after_blocking = repartition_after_blocking

        # TODO: hmmm breaking this flow. Lazy spark ??
        # self._get_spark_from_input_tables_if_not_provided(spark, input_tables)
        self.spark = spark

        if num_partitions_on_repartition is None:
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
        else:
            self.num_partitions_on_repartition = num_partitions_on_repartition

        self._set_catalog_and_database_if_not_provided(catalog, database)

        # TODO: also need to think about where these live:
        # self._drop_splink_cached_tables()
        # self._check_ansi_enabled_if_converting_dates()

        # TODO: (ideally) set things up so databricks can inherit from this
        self.in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
        if self.in_databricks:
            enable_splink(spark)

        self._set_default_break_lineage_method()

        if register_udfs_automatically:
            self._register_udfs_from_jar()

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

    def _setup_for_execute_sql(self, sql, physical_name):
        sql = sqlglot.transpile(sql, read="spark", write="customspark", pretty=True)[0]
        return sql

    def _cleanup_for_execute_sql(self, table, templated_name, physical_name):
        spark_df = self._break_lineage_and_repartition(
            table, templated_name, physical_name
        )

        # After blocking, want to repartition
        # if templated
        spark_df.createOrReplaceTempView(physical_name)

        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        return output_df

    def _run_sql_execution(self, final_sql, templated_name, physical_name):
        return self.spark.sql(final_sql)

    def _delete_table_from_database(self, name):
        self.spark.sql(f"drop table {name}")

    @property
    def accepted_df_dtypes(self):
        return [pd.DataFrame, spark_df]

    # special methods:
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
