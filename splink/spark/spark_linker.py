from pyspark.sql import DataFrame as sp_DataFrame

from splink.linker import Linker, SplinkDataFrame


class SparkDataframe(SplinkDataFrame):
    def __init__(self, df_name, df_value, spark_linker):
        super().__init__(df_name, df_value)
        self.spark_linker = spark_linker

    @property
    def columns(self):
        return list(self.df_value.columns)

    def validate(self):
        if not type(self.df_value) is sp_DataFrame:
            raise ValueError(
                f"{self.df_name} is not a spark dataframe.\n"
                "Spark Linker requires input data"
                " to be spark dataframes"
            )

    def as_record_dict(self):
        return self.df_value.toPandas().to_dict(orient="records")


# These classes want to be as minimal as possible
# dealing with only the backend-specific logic
class SparkLinker(Linker):
    def __init__(self, settings_dict, input_tables):
        df = next(iter(input_tables.values()))

        self.spark = df.sql_ctx.sparkSession

        super().__init__(settings_dict, input_tables)

    def _df_as_obj(self, df_name, df_value):
        return SparkDataframe(df_name, df_value, self)

    def execute_sql(self, sql, df_dict: dict, output_table_name=None, transpile=True):

        # Note no traspilation necessary for Spark as all SQL in Splink is specified
        # as Spark SQL.

        # For each df, register it as table in spark
        for df_obj in df_dict.values():
            df = df_obj.df_value
            table_name = df_obj.df_name
            df.createOrReplaceTempView(table_name)
        output = self.spark.sql(sql)
        output_obj = self._df_as_obj(output_table_name, output)
        return {output_table_name: output_obj}
