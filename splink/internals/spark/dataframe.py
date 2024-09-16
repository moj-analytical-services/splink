from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pandas import DataFrame as PandasDataFrame

from splink.internals.input_column import InputColumn
from splink.internals.splink_dataframe import SplinkDataFrame

from .spark_helpers.custom_spark_dialect import Dialect

logger = logging.getLogger(__name__)

Dialect["customspark"]
if TYPE_CHECKING:
    from .database_api import SparkAPI


class SparkDataFrame(SplinkDataFrame):
    db_api: SparkAPI

    @property
    def columns(self) -> list[InputColumn]:
        sql = f"select * from {self.physical_name} limit 1"
        spark_df = self.db_api._execute_sql_against_backend(sql)

        col_strings = list(spark_df.columns)
        return [InputColumn(c, sqlglot_dialect_str="spark") for c in col_strings]

    def validate(self):
        pass

    def as_record_dict(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.as_pandas_dataframe(limit=limit).to_dict(orient="records")

    def _drop_table_from_database(self, force_non_splink_table=False):
        if self.db_api.break_lineage_method == "delta_lake_table":
            self._check_drop_table_created_by_splink(force_non_splink_table)
            self.db_api.delete_table_from_database(self.physical_name)
        else:
            pass

    def as_pandas_dataframe(self, limit: int = None) -> PandasDataFrame:
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.db_api._execute_sql_against_backend(sql).toPandas()

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
