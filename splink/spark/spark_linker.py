import logging
import sqlglot
from typing import Union, List
import re
import os
import math

from pyspark.sql import Row
from ..linker import Linker
from ..splink_dataframe import SplinkDataFrame
from ..term_frequencies import colname_to_tf_tablename
from ..logging_messages import execute_sql_logging_message_info, log_sql
from ..misc import ensure_is_list
from ..input_column import InputColumn

logger = logging.getLogger(__name__)


class SparkDataframe(SplinkDataFrame):
    def __init__(self, templated_name, physical_name, spark_linker):
        super().__init__(templated_name, physical_name)
        self.spark_linker = spark_linker

    @property
    def columns(self) -> List[InputColumn]:
        sql = f"select * from {self.physical_name} limit 1"
        spark_df = self.spark_linker.spark.sql(sql)

        col_strings = list(spark_df.columns)
        return [InputColumn(c, sql_dialect="spark") for c in col_strings]

    def validate(self):
        pass

    def as_record_dict(self, limit=None):

        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.spark_linker.spark.sql(sql).toPandas().to_dict(orient="records")

    def drop_table_from_database(self, force_non_splink_table=False):

        # Spark, in general, does not persist its results to disk
        # There is a whitelist of dataframes to either perist() or checkpoint()
        # But there's no real need to clean these up, so we'll just do nothing
        pass

    def as_pandas_dataframe(self, limit=None):

        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.spark_linker.spark.sql(sql).toPandas()

    def as_spark_dataframe(self):
        return self.spark_linker.spark.table(self.physical_name)


class SparkLinker(Linker):

    # flake8: noqa: C901
    def __init__(
        self,
        input_table_or_tables,
        settings_dict=None,
        break_lineage_method="parquet",
        set_up_basic_logging=True,
        input_table_aliases: Union[str, list] = None,
        spark=None,
        repartition_after_blocking=False,
        num_partitions_on_repartition=100,
    ):
        """Initialise the linker object, which manages the data linkage process and
        holds the data linkage model.

        Args:
            input_table_or_tables: Input data into the linkage model.  Either a
                single table or a list of tables.  Tables can be provided either as
                a Spark DataFrame, or as the name of the table as a string, as
                registered in the Spark catalog
            settings_dict (dict, optional): A Splink settings dictionary. If not
                provided when the object is created, can later be added using
                `linker.initialise_settings()` Defaults to None.
            break_lineage_method (str, optional): Method to use to cache intermediate
                results.  Can be "checkpoint", "persist" or "parquet".  Defaults to
                "parquet".
            set_up_basic_logging (bool, optional): If true, sets ups up basic logging
                so that Splink sends messages at INFO level to stdout. Defaults to True.
            input_table_aliases (Union[str, list], optional): Labels assigned to
                input tables in Splink outputs.  If the names of the tables in the
                input database are long or unspecific, this argument can be used
                to attach more easily readable/interpretable names. Defaults to None.
            spark: The SparkSession. Required only if `input_table_or_tables` are
                provided as string - otherwise will be inferred from the provided
                Spark Dataframes.
            repartition_after_blocking (bool, optional): In some cases, especially when
                the comparisons are very computationally intensive, performance may be
                improved by repartitioning after blocking to distribute the workload of
                computing the comparison vectors more evenly and reduce the number of
                tasks. Defaults to False.
            num_partitions_on_repartition (int, optional): When saving out intermediate
                results, how many partitions to use?  This should be set so that
                partitions are roughly 100Mb. Defaults to 100.

        """

        if settings_dict is not None and "sql_dialect" not in settings_dict:
            settings_dict["sql_dialect"] = "spark"

        self.break_lineage_method = break_lineage_method

        self.repartition_after_blocking = repartition_after_blocking
        self.num_partitions_on_repartition = num_partitions_on_repartition

        input_tables = ensure_is_list(input_table_or_tables)

        input_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )

        self.spark = spark
        if spark is None:
            for t in input_tables:
                if type(t).__name__ == "DataFrame":
                    self.spark = t.sql_ctx.sparkSession
                    break
        if self.spark is None:
            raise ValueError(
                "If input_table_or_tables are strings rather than "
                "Spark dataframes, you must pass in the spark session using the spark="
                " argument when you initialise the linker"
            )

        for table in self.spark.catalog.listTables():
            if table.isTemporary:
                if "__splink__" in table.name:
                    self.spark.catalog.dropTempView(table.name)

        homogenised_tables = []
        homogenised_aliases = []

        for i, (table, alias) in enumerate(zip(input_tables, input_aliases)):

            if type(alias).__name__ == "DataFrame":
                alias = f"__splink__input_table_{i}"

            if type(table).__name__ == "DataFrame":
                table.createOrReplaceTempView(alias)
                table = alias

            homogenised_tables.append(table)
            homogenised_aliases.append(alias)

        super().__init__(
            homogenised_tables,
            settings_dict,
            set_up_basic_logging,
            input_table_aliases=homogenised_aliases,
        )

    def _table_to_splink_dataframe(self, templated_name, physical_name):
        return SparkDataframe(templated_name, physical_name, self)

    def initialise_settings(self, settings_dict: dict):
        if "sql_dialect" not in settings_dict:
            settings_dict["sql_dialect"] = "spark"
        super().initialise_settings(settings_dict)

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
        ]

        num_partitions = self.num_partitions_on_repartition

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

    def _get_checkpoint_dir_path(self, spark_df):
        # https://github.com/apache/spark/blob/301a13963808d1ad44be5cacf0a20f65b853d5a2/python/pyspark/context.py#L1323 # noqa
        # getCheckpointDir method exists only in Spark 3.1+, use implementation
        # from above link
        if not self.spark._jsc.sc().getCheckpointDir().isEmpty():
            return self.spark._jsc.sc().getCheckpointDir().get()
        else:
            # Raise checkpointing error
            spark_df.limit(1).checkpoint()

    def _break_lineage_and_repartition(self, spark_df, templated_name, physical_name):

        spark_df = self._repartition_if_needed(spark_df, templated_name)

        regex_to_persist = [
            r"__splink__df_comparison_vectors",
            r"__splink__df_concat_with_tf",
            r"__splink__df_predict",
            r"__splink__df_tf_.+",
            r"__splink__df_representatives",
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
            else:
                raise ValueError(
                    f"Unknown break_lineage_method: {self.break_lineage_method}"
                )
        return spark_df

    def _execute_sql_against_backend(
        self, sql, templated_name, physical_name, transpile=True
    ):

        if transpile:
            sql = sqlglot.transpile(sql, read=None, write="spark", pretty=True)[0]

        spark_df = self.spark.sql(sql)
        logger.debug(execute_sql_logging_message_info(templated_name, physical_name))
        logger.log(5, log_sql(sql))
        spark_df = self._break_lineage_and_repartition(
            spark_df, templated_name, physical_name
        )

        # After blocking, want to repartition
        # if templated
        spark_df.createOrReplaceTempView(physical_name)

        output_df = self._table_to_splink_dataframe(templated_name, physical_name)
        return output_df

    def _random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        return f" TABLESAMPLE ({percent} PERCENT) "

    def _table_exists_in_database(self, table_name):
        tables = self.spark.catalog.listTables()
        for t in tables:
            if t.name == table_name:
                return True
        return False

    def _records_to_table(self, records, as_table_name):
        df = self.spark.createDataFrame(Row(**x) for x in records)
        df.createOrReplaceTempView(as_table_name)

    def register_tf_table(self, df, col_name):
        df.createOrReplaceTempView(colname_to_tf_tablename(col_name))
