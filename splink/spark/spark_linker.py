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
    def __init__(
        self,
        input_table_or_tables,
        settings_dict=None,
        break_lineage_method="persist",
        set_up_basic_logging=True,
        input_table_aliases: Union[str, list] = None,
        spark=None,
        break_lineage_after_blocking=False,
        num_partitions_on_repartition=100,
    ):

        if settings_dict is not None and "sql_dialect" not in settings_dict:
            settings_dict["sql_dialect"] = "spark"

        self.break_lineage_method = break_lineage_method

        self.break_lineage_after_blocking = break_lineage_after_blocking
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

    # flake8: noqa: C901
    def _break_lineage(self, spark_df, templated_name, physical_name):

        regex_to_persist = [
            r"__splink__df_comparison_vectors",
            r"__splink__df_concat_with_tf",
            r"__splink__df_predict",
            r"__splink__df_tf_.+",
            r"__splink__df_representatives_.+",
            r"__splink__df_neighbours",
            r"__splink__df_connected_components_df",
        ]

        if self.break_lineage_after_blocking:
            regex_to_persist.append(r"__splink__df_blocked")

        names_to_repartition = [
            r"__splink__df_comparison_vectors",
            r"__splink__df_blocked",
            r"__splink__df_neighbours",
            r"__splink__df_representatives_.+",
        ]

        num_partitions = self.num_partitions_on_repartition
        if re.match(r"__splink__df_representatives_.+", templated_name):
            num_partitions = math.ceil(self.num_partitions_on_repartition / 6)

        if re.match(r"|".join(regex_to_persist), templated_name):
            if self.break_lineage_method == "persist":

                spark_df = spark_df.persist()

                logger.debug(f"persisted {templated_name}")
            elif self.break_lineage_method == "checkpoint":
                if re.match(r"|".join(names_to_repartition), templated_name):
                    spark_df = spark_df.repartition(num_partitions)
                spark_df = spark_df.checkpoint()
                logger.debug(f"Checkpointed {templated_name}")
            elif self.break_lineage_method == "parquet":
                # https://github.com/apache/spark/blob/301a13963808d1ad44be5cacf0a20f65b853d5a2/python/pyspark/context.py#L1323 # noqa
                # getCheckpointDir method exists only in Spark 3.1+, use implementation
                # from above link
                if not self.spark._jsc.sc().getCheckpointDir().isEmpty():
                    checkpoint_dir = self.spark._jsc.sc().getCheckpointDir().get()
                else:
                    # Raise checkpointing error
                    spark_df.limit(1).checkpoint()
                write_path = os.path.join(checkpoint_dir, physical_name)
                if re.match(r"|".join(names_to_repartition), templated_name):
                    spark_df = spark_df.repartition(num_partitions)
                spark_df.write.mode("overwrite").parquet(write_path)

                spark_df = self.spark.read.parquet(write_path)
                logger.debug(f"Parqueted {templated_name}")
            else:
                raise ValueError(
                    f"Unknown break_lineage_method: {self.break_lineage_method}"
                )
        return spark_df

    def _execute_sql(self, sql, templated_name, physical_name, transpile=True):

        if transpile:
            sql = sqlglot.transpile(sql, read=None, write="spark", pretty=True)[0]

        spark_df = self.spark.sql(sql)
        logger.debug(execute_sql_logging_message_info(templated_name, physical_name))
        logger.log(5, log_sql(sql))
        spark_df = self._break_lineage(spark_df, templated_name, physical_name)

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
