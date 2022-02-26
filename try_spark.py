from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from splink.spark.spark_linker import SparkLinker
from try_settings import settings_dict

conf = SparkConf()
conf.set("spark.driver.memory", "12g")
conf.set("spark.sql.shuffle.partitions", "8")
conf.set("spark.default.parallelism", "8")

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)
df.createOrReplaceTempView("df")
tables = spark.catalog.listTables()

tables
linker = SparkLinker(settings_dict, input_tables={"fake_data_1": df})


linker.train_u_using_random_sampling(target_rows=1e6)

blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
linker.train_m_using_expectation_maximisation(blocking_rule)

blocking_rule = "l.dob = r.dob"
linker.train_m_using_expectation_maximisation(blocking_rule)

df = linker.predict()
df_pd = df.as_pandas_dataframe()
df_pd.sort_values(["unique_id_l", "unique_id_r"])
