# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# COMMAND ----------

import splink.spark.spark_comparison_library as cl
from splink.spark.enable_splink import enable_splink

# COMMAND ----------

# this attachs the splink JAR to the cluster
enable_splink(spark)

# COMMAND ----------

df = spark.read.csv(
  "file:/Workspace/Repos/robert.whiffin@databricks.com/splink/tests/datasets/fake_1000_from_splink_demos.csv"
  , header=True
)

# COMMAND ----------

settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        cl.jaro_winkler_at_thresholds("first_name", 0.8),
        cl.jaro_winkler_at_thresholds("surname", 0.8),
        cl.levenshtein_at_thresholds("dob"),
        cl.exact_match("city", term_frequency_adjustments=True),
        cl.levenshtein_at_thresholds("email"),
    ],
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
    "em_convergence": 0.01
}

# COMMAND ----------

# this defines where we are going to store the splink tables
catalog_name = "splink"
database_name = user_name
spark.sql(f"create catalog if not exists {catalog_name};")
spark.sql(f"use catalog {catalog_name};")
spark.sql(f"create database if not exists {database_name};")
spark.sql(f"use database {database_name};")

# COMMAND ----------

from splink.databricks.spark_linker import SparkLinker
linker = SparkLinker(df, catalog = "splink", schema = "robert_whiffin", settings_dict=settings, spark=spark)
deterministic_rules = [
    "l.first_name = r.first_name and levenshtein(r.dob, l.dob) <= 1",
    "l.surname = r.surname and levenshtein(r.dob, l.dob) <= 1",
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    "l.email = r.email"
]

# COMMAND ----------

linker.estimate_probability_two_random_records_match(deterministic_rules, recall=0.6)

# COMMAND ----------

linker.estimate_u_using_random_sampling(target_rows=5e5)

# COMMAND ----------

spark.udf.registerJavaFunction(
    "jaro_winkler", "uk.gov.moj.dash.linkage.JaroWinklerSimilarity", types.DoubleType()
)

# COMMAND ----------

training_blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
training_session_fname_sname = linker.estimate_parameters_using_expectation_maximisation(training_blocking_rule)

training_blocking_rule = "l.dob = r.dob"
training_session_dob = linker.estimate_parameters_using_expectation_maximisation(training_blocking_rule)

# COMMAND ----------

results = linker.predict(threshold_match_probability=0.9)

# COMMAND ----------

results.as_pandas_dataframe(limit=5)
