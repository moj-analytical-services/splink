from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..input_column import InputColumn
from ..linker import Linker
from ..splink_dataframe import SplinkDataFrame
from ..term_frequencies import colname_to_tf_tablename
from .spark_helpers.custom_spark_dialect import Dialect

logger = logging.getLogger(__name__)

Dialect["customspark"]
if TYPE_CHECKING:
    from ..database_api import SparkAPI


class SparkDataFrame(SplinkDataFrame):
    db_api: SparkAPI

    @property
    def columns(self) -> list[InputColumn]:
        sql = f"select * from {self.physical_name} limit 1"
        spark_df = self.db_api.spark.sql(sql)

        col_strings = list(spark_df.columns)
        return [InputColumn(c, sql_dialect="spark") for c in col_strings]

    def validate(self):
        pass

    def as_record_dict(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.db_api.spark.sql(sql).toPandas().to_dict(orient="records")

    def _drop_table_from_database(self, force_non_splink_table=False):
        if self.db_api.break_lineage_method == "delta_lake_table":
            self._check_drop_table_created_by_splink(force_non_splink_table)
            self.db_api._delete_table_from_database(self.physical_name)
        else:
            pass

    def as_pandas_dataframe(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.db_api.spark.sql(sql).toPandas()

    def as_spark_dataframe(self):
        return self.db_api.spark.table(self.physical_name)

    def to_parquet(self, filepath, overwrite=False):
        if not overwrite:
            self.check_file_exists(filepath)

        spark_df = self.as_spark_dataframe()
        spark_df.write.mode("overwrite").format("parquet").save(filepath)

    def to_csv(self, filepath, overwrite=False):
        if not overwrite:
            self.check_file_exists(filepath)

        spark_df = self.as_spark_dataframe()
        spark_df.write.mode("overwrite").format("csv").option("header", "true").save(
            filepath
        )


class SparkLinker(Linker):
    def _drop_splink_cached_tables(self):
        # Clean up Splink cache that may exist from any previous splink session

        # if we use spark.sql("USE DATABASE db") commands we change the default. This
        # approach prevents side effects.
        splink_tables = self.spark.sql(
            f"show tables from {self.splink_data_store} like '__splink__*'"
        )
        temp_tables = splink_tables.filter("isTemporary").collect()
        drop_tables = list(
            map(lambda x: x.tableName, filter(lambda x: x.isTemporary, temp_tables))
        )
        # drop old temp tables
        # specifying a catalog and database doesn't work for temp tables.
        for x in drop_tables:
            self.spark.sql(f"drop table {x}")

    def register_tf_table(self, df, col_name, overwrite=False):
        self.register_table(df, colname_to_tf_tablename(col_name), overwrite)

    def _check_ansi_enabled_if_converting_dates(self):
        # because have this code in the init- need to first check if settings dict exits
        try:
            comparisons_as_list = self._settings_obj._settings_dict["comparisons"]
            settings_obj = True
        except ValueError:
            settings_obj = False

        if settings_obj is True:
            # see if any of the comparisons contain 'to_timestamp',
            #  the spark SQL used to convert date to str if date-to-str cast is used
            if any(
                [
                    "to_timestamp" in str(comparisons_as_list[x].values())
                    for x in range(0, len(comparisons_as_list))
                ]
            ):
                # now check if ansi is enabled:
                bool_ansi = self.spark.sparkContext.getConf().get(
                    "spark.sql.ansi.enabled"
                )
                if bool_ansi == "False" or bool_ansi is None:
                    logger.warning(
                        """--WARN-- \n You are using datediff comparison
                        with str-casting and ANSI is not enabled. Bad dates
                        e.g. 1999-13-54 will not trigger an exception but will
                        classed as comparison level = "ELSE". Ensure date strings
                        are cleaned to remove bad dates \n"""
                    )
