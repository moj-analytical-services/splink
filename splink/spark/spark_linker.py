import logging
from typing import Union
import re
from pyspark.sql import Row
from ..linker import Linker
from ..splink_dataframe import SplinkDataFrame
from ..term_frequencies import colname_to_tf_tablename
from ..logging_messages import execute_sql_logging_message_info, log_sql

logger = logging.getLogger(__name__)


class SparkDataframe(SplinkDataFrame):
    def __init__(self, templated_name, physical_name, spark_linker):
        super().__init__(templated_name, physical_name)
        self.spark_linker = spark_linker

    @property
    def columns(self):
        sql = f"select * from {self.physical_name} limit 1"
        spark_df = self.spark_linker.spark.sql(sql)

        return list(spark_df.columns)

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
        persist_level=None,
        set_up_basic_logging=True,
        input_table_aliases: Union[str, list] = None,
        spark=None,
    ):

        self.break_lineage_method = break_lineage_method
        self.persist_level = persist_level

        input_tables = self._ensure_is_list(input_table_or_tables)

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
                " argument when you initialise thel inker"
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

    def _df_as_obj(self, templated_name, physical_name):
        return SparkDataframe(templated_name, physical_name, self)

    def _break_lineage(self, spark_df, templated_name):

        regex_to_persist = [
            r"__splink__df_comparison_vectors",
            r"__splink__df_concat_with_tf",
            r"__splink__df_predict",
            r"__splink__df_tf_.+",
        ]

        if re.match(r"|".join(regex_to_persist), templated_name):

            if self.break_lineage_method == "persist":
                if self.persist_level is None:
                    spark_df = spark_df.persist()
                else:
                    spark_df = spark_df.persist(self.persist_level)
                logger.debug(f"persisted {templated_name}")
            elif self.break_lineage_method == "checkpoint":
                spark_df = spark_df.checkpoint()
                logger.debug(f"checkpointed {templated_name}")
            else:
                raise ValueError(
                    f"Unknown break_lineage_method: {self.break_lineage_method}"
                )
        return spark_df

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):

        spark_df = self.spark.sql(sql)
        logger.debug(execute_sql_logging_message_info(templated_name, physical_name))
        logger.log(5, log_sql(sql))
        spark_df = self._break_lineage(spark_df, templated_name)

        spark_df.createOrReplaceTempView(physical_name)

        output_df = self._df_as_obj(templated_name, physical_name)
        return output_df

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        return f" TABLESAMPLE ({percent} PERCENT) "

    def table_exists_in_database(self, table_name):
        tables = self.spark.catalog.listTables()
        for t in tables:
            if t.name == table_name:
                return True
        return False

    def records_to_table(self, records, as_table_name):
        df = self.spark.createDataFrame(Row(**x) for x in records)
        df.createOrReplaceTempView(as_table_name)

    def register_tf_table(self, df, col_name):
        df.createOrReplaceTempView(colname_to_tf_tablename(col_name))
