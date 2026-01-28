# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Linking in Spark
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/spark/deduplicate_1k_synthetic.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink
# # !pip install pyspark

# %%
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from splink.backends.spark import similarity_jar_location

conf = SparkConf()
# This parallelism setting is only suitable for a small toy example
conf.set("spark.driver.memory", "12g")
conf.set("spark.default.parallelism", "8")
conf.set("spark.sql.codegen.wholeStage", "false")


# Add custom similarity functions, which are bundled with Splink
# documented here: https://github.com/moj-analytical-services/splink_scalaudfs
path = similarity_jar_location()
conf.set("spark.jars", path)

sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession(sc)
spark.sparkContext.setCheckpointDir("./tmp_checkpoints")

# %% tags=["hide_input", "hide_output"]
# Disable warnings for pyspark - you don't need to include this
import warnings

spark.sparkContext.setLogLevel("ERROR")
warnings.simplefilter("ignore", UserWarning)

# %%
from splink import splink_datasets

pandas_df = splink_datasets.fake_1000

df = spark.createDataFrame(pandas_df)

# %%
import splink.comparison_library as cl
from splink import Linker, SettingsCreator, SparkAPI, block_on

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.LevenshteinAtThresholds(
            "dob"
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        "l.surname = r.surname",  # alternatively, you can write BRs in their SQL form
    ],
    retain_intermediate_calculation_columns=True,
    em_convergence=0.01,
)

# %%
db_api = SparkAPI(spark_session=spark)
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)
deterministic_rules = [
    "l.first_name = r.first_name and levenshtein(r.dob, l.dob) <= 1",
    "l.surname = r.surname and levenshtein(r.dob, l.dob) <= 1",
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    "l.email = r.email",
]

linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.6)

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=5e5)

# %%
training_blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
training_session_fname_sname = (
    linker.training.estimate_parameters_using_expectation_maximisation(training_blocking_rule)
)

training_blocking_rule = "l.dob = r.dob"
training_session_dob = linker.training.estimate_parameters_using_expectation_maximisation(
    training_blocking_rule
)

# %%
results = linker.inference.predict(threshold_match_probability=0.9)

# %%
spark_df = results.as_spark_dataframe().show()
