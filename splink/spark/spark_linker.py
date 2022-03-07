from pyspark.sql import Row
from ..linker import Linker, SplinkDataFrame
from ..term_frequencies import colname_to_tf_tablename


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
    def __init__(self, settings_dict=None, input_tables={}):
        df = next(iter(input_tables.values()))

        self.spark = df.sql_ctx.sparkSession

        for templated_name, df in input_tables.items():
            db_tablename = f"__splink__{templated_name}"

            df.createOrReplaceTempView(db_tablename)
            input_tables[templated_name] = db_tablename

        super().__init__(settings_dict, input_tables)

    def _df_as_obj(self, templated_name, physical_name):
        return SparkDataframe(templated_name, physical_name, self)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):

        spark_df = self.spark.sql(sql)

        if templated_name in (
            "__splink__df_comparison_vectors",
            "__splink__df_concat_with_tf",
        ):
            spark_df.persist()
        if templated_name.startswith("__splink__df_tf_"):
            if physical_name.startswith("__splink__df_tf_"):
                spark_df.persist()

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
