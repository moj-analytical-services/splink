from pyspark.sql import DataFrame as sp_DataFrame

from splink.linker import Linker, SplinkDataFrame


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

    def as_pandas_dataframe(self):

        sql = f"select * from {self.physical_name}"

        return self.spark_linker.spark.sql(sql).toPandas()


# These classes want to be as minimal as possible
# dealing with only the backend-specific logic
class SparkLinker(Linker):
    def __init__(self, settings_dict, input_tables, tf_tables={}):
        df = next(iter(input_tables.values()))

        self.spark = df.sql_ctx.sparkSession

        for templated_name, df in input_tables.items():
            # Make a table with this name
            df.createOrReplaceTempView(templated_name)

            input_tables[templated_name] = templated_name

        for templated_name, df in tf_tables.items():
            # Make a table with this name

            df.createOrReplaceTempView(templated_name)
            tf_tables[templated_name] = templated_name

        super().__init__(settings_dict, input_tables)

    def _df_as_obj(self, templated_name, physical_name):
        return SparkDataframe(templated_name, physical_name, self)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):

        # print(sql)
        spark_df = self.spark.sql(sql)

        # Break lineage
        spark_df.write.mode("overwrite").parquet(f"./tmp_spark/{physical_name}")
        spark_df = self.spark.read.parquet(f"./tmp_spark/{physical_name}")

        spark_df.createOrReplaceTempView(physical_name)

        output_df = self._df_as_obj(templated_name, physical_name)
        return output_df

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        return f" TABLESAMPLE ({percent} PERCENT) "
