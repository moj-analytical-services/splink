from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession
from splink.spark.spark_linker import SparkLinker
from try_settings import settings_dict

settings_dict["max_iterations"] = 2
conf = SparkConf()
conf.set("spark.driver.memory", "12g")
conf.set("spark.sql.shuffle.partitions", "8")
conf.set("spark.default.parallelism", "8")

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)


linker = SparkLinker(settings_dict, input_tables={"fake_data_1": df})
linker.column_frequency_chart(
    ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
)
linker.compute_tf_table("city")
linker.compute_tf_table("first_name")

linker.train_u_using_random_sampling(target_rows=1e6)

blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
linker.train_m_using_expectation_maximisation(blocking_rule)

blocking_rule = "l.dob = r.dob"
linker.train_m_using_expectation_maximisation(blocking_rule)

df = linker.predict()
df_pd = df.as_pandas_dataframe()
df_pd.sort_values(["unique_id_l", "unique_id_r"])

# spark.catalog.listTables()
record_1 = {
    "source_dataset": "fake_data_1",
    "unique_id": 0,
    "first_name": "Jules ",
    "surname": "",
    "dob": "2015-10-29",
    "city": "London",
    "email": "hannah88@powers.com",
    "group": 0,
}

record_2 = {
    "unique_id": 1,
    "first_name": "Julia ",
    "surname": "Taylor",
    "dob": "2015-07-31",
    "city": "London",
    "email": "hannah88@powers.com",
    "group": 0,
}

linker.compare_two_records(record_1, record_2).as_pandas_dataframe()

linker.find_matches_to_new_records([record_1]).as_pandas_dataframe()
