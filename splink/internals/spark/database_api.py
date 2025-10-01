import logging
import math
import os
import re

import pandas as pd
import sqlglot
from numpy import nan
from pyspark.sql.dataframe import DataFrame as spark_df
from pyspark.sql.utils import AnalysisException

from splink.internals.database_api import AcceptableInputTableType, DatabaseAPI
from splink.internals.databricks.enable_splink import enable_splink
from splink.internals.dialects import (
    SparkDialect,
)
from splink.internals.misc import (
    major_minor_version_greater_equal_than,
)

from .dataframe import SparkDataFrame
from .jar_location import get_scala_udfs

logger = logging.getLogger(__name__)


class SparkAPI(DatabaseAPI[spark_df]):
    sql_dialect = SparkDialect()

    def __init__(
        self,
        *,
        spark_session,
        break_lineage_method=None,
        catalog=None,
        database=None,
        repartition_after_blocking=False,
        num_partitions_on_repartition=None,
        register_udfs_automatically=True,
    ):
        super().__init__()
        self.break_lineage_method = break_lineage_method

        # these properties will be needed whenever spark is _actually_ set up
        self.repartition_after_blocking = repartition_after_blocking
        self.spark = spark_session

        if num_partitions_on_repartition:
            self.num_partitions_on_repartition = num_partitions_on_repartition
        else:
            self.set_default_num_partitions_on_repartition_if_missing()

        self._set_splink_datastore(catalog, database)

        self.in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
        if self.in_databricks:
            enable_splink(self.spark)

        if register_udfs_automatically:
            self._register_udfs_from_jar()

        self._set_default_break_lineage_method()

    def _table_registration(
        self, input: AcceptableInputTableType, table_name: str
    ) -> None:
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = self.spark.createDataFrame(input)

        if isinstance(input, pd.DataFrame):
            input = self._clean_pandas_df(input)
            input = self.spark.createDataFrame(input)

        input.createOrReplaceTempView(table_name)

    def table_to_splink_dataframe(
        self, templated_name: str, physical_name: str
    ) -> SparkDataFrame:
        return SparkDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        query_result = self._execute_sql_against_backend(
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
    ) -> SparkDataFrame:
        spark_df = self._break_lineage_and_repartition(
            table, templated_name, physical_name
        )

        # After blocking, want to repartition
        # if templated
        spark_df.createOrReplaceTempView(physical_name)

        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        return output_df

    def _execute_sql_against_backend(self, final_sql: str) -> spark_df:
        return self.spark.sql(final_sql)

    def delete_table_from_database(self, name):
        self._execute_sql_against_backend(f"drop table {name}")

    @property
    def accepted_df_dtypes(self):
        return [pd.DataFrame, spark_df]

    def _clean_pandas_df(self, df):
        return df.fillna(nan).replace([nan, pd.NA], [None, None])

    def _set_splink_datastore(self, catalog, database):
        # spark.catalog.currentCatalog() is not available in versions of spark before
        # 3.4.0. In Spark versions less that 3.4.0 we will require explicit catalog
        # setting, but will revert to default in Spark versions greater than 3.4.0
        threshold = "3.4.0"

        if (
            major_minor_version_greater_equal_than(self.spark.version, threshold)
            and not catalog
        ):
            # set the catalog and database of where to write output tables
            catalog = (
                catalog if catalog is not None else self.spark.catalog.currentCatalog()
            )
        database = (
            database if database is not None else self.spark.catalog.currentDatabase()
        )

        # this defines the catalog.database location where splink's data outputs will
        # be stored. The filter will remove none, so if catalog is not provided and
        # spark version is < 3.3.0 we will use the default catalog.
        self.splink_data_store = ".".join(
            [self._quote_if_needed(x) for x in [catalog, database] if x is not None]
        )

    def _quote_if_needed(self, identifier):
        if identifier.startswith("`") and identifier.endswith("`"):
            return identifier
        return f"`{identifier}`"

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
                ":\nfrom splink.backends.spark import similarity_jar_location"
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
            r"__splink__blocked_id_pairs",
            r"__splink__nodes_in_play",
            r"__splink__edges_in_play",
            r"__splink__clusters_at_threshold",
            r"__splink__clusters_at_all_thresholds",
            r"__splink__stable_nodes_at_new_threshold",
            r"__splink__clustering_output_final",
        ]

        num_partitions = self.num_partitions_on_repartition

        if templated_name == "__splink__df_predict":
            num_partitions = math.ceil(num_partitions)
        elif templated_name == "__splink__df_representatives":
            num_partitions = math.ceil(num_partitions / 6)
        elif templated_name == "__splink__df_neighbours":
            num_partitions = math.ceil(num_partitions / 4)
        elif templated_name == "__splink__df_concat_with_tf_sample":
            num_partitions = math.ceil(num_partitions / 4)
        elif templated_name == "__splink__df_concat_with_tf":
            num_partitions = math.ceil(num_partitions / 4)
        elif templated_name == "__splink__blocked_id_pairs":
            num_partitions = math.ceil(num_partitions / 6)
        elif templated_name == "__splink__distinct_clusters_at_threshold":
            num_partitions = 1
        elif templated_name == "__splink__nodes_in_play":
            num_partitions = math.ceil(num_partitions / 10)
        elif templated_name == "__splink__edges_in_play":
            num_partitions = math.ceil(num_partitions / 10)
        elif templated_name == "__splink__clusters_at_threshold":
            num_partitions = math.ceil(num_partitions / 10)
        elif templated_name == "__splink__clusters_at_all_thresholds":
            num_partitions = math.ceil(num_partitions / 10)
        elif templated_name == "__splink__stable_nodes_at_new_threshold":
            num_partitions = math.ceil(num_partitions / 10)
        elif templated_name == "__splink__clustering_output_final":
            num_partitions = math.ceil(num_partitions / 10)

        if re.fullmatch(r"|".join(names_to_repartition), templated_name):
            spark_df = spark_df.repartition(num_partitions)

        return spark_df

    def _break_lineage_and_repartition(self, spark_df, templated_name, physical_name):
        spark_df = self._repartition_if_needed(spark_df, templated_name)

        regex_to_persist = [
            r"__splink__df_comparison_vectors",
            r"__splink__df_concat_sample",
            r"__splink__df_concat_with_tf",
            r"__splink__df_predict",
            r"__splink__df_tf_.+",
            r"__splink__df_representatives.*",
            r"__splink__representatives.*",
            r"__splink__df_neighbours",
            r"__splink__df_connected_components_df",
            r"__splink__blocked_id_pairs",
            r"__splink__marginal_exploded_ids_blocking_rule.*",
            r"__splink__nodes_in_play",
            r"__splink__edges_in_play",
            r"__splink__clusters_at_threshold",
            r"__splink__distinct_clusters_at_threshold",
            r"__splink__clusters_at_all_thresholds",
            r"__splink__clustering_output_final",
            r"__splink__stable_nodes_at_new_threshold",
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

        if templated_name == "__splink__blocked_id_pairs":
            spark_df = spark_df.repartition(self.num_partitions_on_repartition)

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
