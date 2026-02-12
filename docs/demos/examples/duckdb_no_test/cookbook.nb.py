# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Cookbook
#
# This notebook contains a miscellaneous collection of runnable examples illustrating various Splink techniques.

# %% [markdown]
# ## Array columns
#
# ### Comparing array columns
#
# This example shows how we can use use `ArrayIntersectAtSizes` to assess the similarity of columns containing arrays.

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %% tags=["hide_input", "hide_output"]
import logging
logging.getLogger("splink").setLevel(logging.ERROR)


# %%
import pandas as pd

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on


data = [
    {"unique_id": 1, "first_name": "John", "postcode": ["A", "B"]},
    {"unique_id": 2, "first_name": "John", "postcode": ["B"]},
    {"unique_id": 3, "first_name": "John", "postcode": ["A"]},
    {"unique_id": 4, "first_name": "John", "postcode": ["A", "B"]},
    {"unique_id": 5, "first_name": "John", "postcode": ["C"]},
]

df = pd.DataFrame(data)

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
    ],
    comparisons=[
        cl.ArrayIntersectAtSizes("postcode", [2, 1]),
        cl.ExactMatch("first_name"),
    ]
)


db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings, set_up_basic_logging=False)

linker.inference.predict().as_pandas_dataframe()

# %% [markdown]
# ### Blocking on array columns
#
# This example shows how we can use `block_on` to block on the individual elements of an array column - that is, pairwise comaprisons are created for pairs or records where any of the elements in the array columns match.

# %%
import pandas as pd

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on


data = [
    {"unique_id": 1, "first_name": "John", "postcode": ["A", "B"]},
    {"unique_id": 2, "first_name": "John", "postcode": ["B"]},
    {"unique_id": 3, "first_name": "John", "postcode": ["C"]},

]

df = pd.DataFrame(data)

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("postcode", arrays_to_explode=["postcode"]),
    ],
    comparisons=[
        cl.ArrayIntersectAtSizes("postcode", [2, 1]),
        cl.ExactMatch("first_name"),
    ]
)


db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings, set_up_basic_logging=False)

linker.inference.predict().as_pandas_dataframe()

# %% [markdown]
# ## Other
#

# %% [markdown]
# ### Using DuckDB without pandas
#
# In this example, we read data directly using DuckDB and obtain results in native DuckDB `DuckDBPyRelation` format.
#

# %%
import duckdb
import tempfile
import os

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

# Create a parquet file on disk to demontrate native DuckDB parquet reading
df = splink_datasets.fake_1000
temp_file = tempfile.NamedTemporaryFile(delete=True, suffix=".parquet")
temp_file_path = temp_file.name
df.to_parquet(temp_file_path)

# Example would start here if you already had a parquet file
duckdb_df = duckdb.read_parquet(temp_file_path)

db_api = DuckDBAPI(":default:")
df_sdf = db_api.register(df)
settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.NameComparison("first_name"),
        cl.JaroAtThresholds("surname"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name", "dob"),
        block_on("surname"),
    ],
)

linker = Linker(df_sdf, settings, set_up_basic_logging=False)

result = linker.inference.predict().as_duckdbpyrelation()

# Since result is a DuckDBPyRelation, we can use all the usual DuckDB API
# functions on it.

# For example, we can use the `sort` function to sort the results,
# or could use result.to_parquet() to write to a parquet file.
result.sort("match_weight")

# %% [markdown]
# ### Fixing `m` or `u` probabilities during training
#

# %%
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets


db_api = DuckDBAPI()

first_name_comparison = cl.CustomComparison(
    comparison_levels=[
        cll.NullLevel("first_name"),
        cll.ExactMatchLevel("first_name").configure(
            m_probability=0.9999,
            fix_m_probability=True,
            u_probability=0.7,
            fix_u_probability=True,
        ),
        cll.ElseLevel(),
    ]
)
settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        first_name_comparison,
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("dob"),
    ],
    additional_columns_to_retain=["cluster"],
)

df = splink_datasets.fake_1000
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings, set_up_basic_logging=False)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

linker.visualisations.m_u_parameters_chart()

# %% [markdown]
# ### Manually altering `m` and `u` probabilities post-training
#
# This is not officially supported, but can be useful for ad-hoc alterations to trained models.

# %%
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets
from splink.datasets import splink_dataset_labels

labels = splink_dataset_labels.fake_1000_labels

db_api = DuckDBAPI()


settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("dob"),
    ],
)
df = splink_datasets.fake_1000
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings, set_up_basic_logging=False)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))


surname_comparison = linker._settings_obj._get_comparison_by_output_column_name(
    "surname"
)
else_comparison_level = (
    surname_comparison._get_comparison_level_by_comparison_vector_value(0)
)
else_comparison_level._m_probability = 0.1


linker.visualisations.m_u_parameters_chart()

# %% [markdown]
# ### Generate the (beta) labelling tool

# %%
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

db_api = DuckDBAPI()

df = splink_datasets.fake_1000

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.ExactMatch("email"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    max_iterations=2,
)

df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings, set_up_basic_logging=False)

linker.training.estimate_probability_two_random_records_match(
    [block_on("first_name", "surname")], recall=0.7
)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

pairwise_predictions = linker.inference.predict(threshold_match_weight=-10)

first_unique_id = df_sdf.as_record_dict(limit=1)[0]["unique_id"]
linker.evaluation.labelling_tool_for_specific_record(unique_id=first_unique_id, overwrite=True)


# %% [markdown]
# ### Modifying settings after loading from a serialised `.json` model

# %%
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

# setup to create a model

db_api = DuckDBAPI()

df = splink_datasets.fake_1000

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.LevenshteinAtThresholds("first_name"),
        cl.LevenshteinAtThresholds("surname"),

    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name", "dob"),
        block_on("surname"),
    ]
)

df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)


linker.misc.save_model_to_json("mod.json", overwrite=True)

new_settings = SettingsCreator.from_path_or_dict("mod.json")

new_settings.retain_intermediate_calculation_columns = True
new_settings.blocking_rules_to_generate_predictions = ["1=1"]
new_settings.additional_columns_to_retain = ["cluster"]
db_api_new = DuckDBAPI()
df_sdf_new = db_api_new.register(df)
linker = Linker(df_sdf_new, new_settings)


linker.inference.predict().as_duckdbpyrelation().show()

# %% [markdown]
# ### Using a DuckDB UDF in a comparison level

# %%
import difflib

import duckdb
from duckdb.sqltypes import VARCHAR, DOUBLE

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets


def custom_partial_ratio(s1, s2):
    """Custom function to compute partial ratio similarity between two strings."""
    s1, s2 = str(s1), str(s2)
    matcher = difflib.SequenceMatcher(None, s1, s2)
    return matcher.ratio()


df = splink_datasets.fake_1000

con = duckdb.connect()
con.create_function(
    "custom_partial_ratio",
    custom_partial_ratio,
    [duckdb.sqltypes.VARCHAR, duckdb.sqltypes.VARCHAR],
    duckdb.sqltypes.DOUBLE,
)
db_api = DuckDBAPI(connection=con)


fuzzy_email_comparison = {
    "output_column_name": "email_fuzzy",
    "comparison_levels": [
        cll.NullLevel("email"),
        cll.ExactMatchLevel("email"),
        {
            "sql_condition": "custom_partial_ratio(email_l, email_r) > 0.8",
            "label_for_charts": "Fuzzy match (≥ 0.8)",
        },
        cll.ElseLevel(),
    ],
}

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        fuzzy_email_comparison,
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    max_iterations=2,
)

df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)

linker.training.estimate_probability_two_random_records_match(
    [block_on("first_name", "surname")], recall=0.7
)

linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

pairwise_predictions = linker.inference.predict(threshold_match_weight=-10)


# %% [markdown]
# ### Nested linkage
#
# In this example, we want to deduplicate persons but only within each company.
#
# The problem is that the companies themselves may be duplicates, so we proceed by deduplicating the companies first and then deduplicating persons nested within each company we resolved in step 1.
#
# Note I do not include full model training code here, just a simple/illustrative model spec.  The example is more about demonstrating the nested linkage process.

# %%
import duckdb
import pandas as pd
import os
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink.clustering import cluster_pairwise_predictions_at_threshold

# Example data with companies and persons
company_person_records_list = [
    {
        "unique_id": 1001,
        "client_id": "GGN1",
        "company_name": "Green Garden Nurseries Ltd",
        "postcode": "NR1 1AB",
        "person_firstname": "John",
        "person_surname": "Smith",
    },
    {
        "unique_id": 1002,
        "client_id": "GGN1",
        "company_name": "Green Gardens Ltd",
        "postcode": "NR1 1AB",
        "person_firstname": "Sarah",
        "person_surname": "Jones",
    },
    {
        "unique_id": 1003,
        "client_id": "GGN2",
        "company_name": "Green Garden Nurseries Ltd",
        "postcode": "NR1 1AB",
        "person_firstname": "John",
        "person_surname": "Smith",
    },
    {
        "unique_id": 3001,
        "client_id": "GW1",
        "company_name": "Garden World",
        "postcode": "LS2 3EF",
        "person_firstname": "Emma",
        "person_surname": "Wilson",
    },
    {
        "unique_id": 3002,
        "client_id": "GW1",
        "company_name": "Garden World UK",
        "postcode": "LS2 3EF",
        "person_firstname": "Emma",
        "person_surname": "Wilson",
    },
    {
        "unique_id": 3003,
        "client_id": "GW2",
        "company_name": "Garden World",
        "postcode": "LS2 3EF",
        "person_firstname": "Emma",
        "person_surname": "Wilson",
    },
    {
        "unique_id": 3004,
        "client_id": "GW2",
        "company_name": "Garden World",
        "postcode": "LS2 3EF",
        "person_firstname": "James",
        "person_surname": "Taylor",
    },
]
company_person_records = pd.DataFrame(company_person_records_list)
company_person_records
print("========== NESTED COMPANY-PERSON LINKAGE EXAMPLE ==========")
print("This example demonstrates a two-phase linkage process:")
print("1. First, link and cluster to find duplicate companies (client_id)")
print("2. Then, deduplicate persons ONLY within each company cluster")

# Initialize database
if os.path.exists("nested_linkage.ddb"):
    os.remove("nested_linkage.ddb")
con = duckdb.connect("nested_linkage.ddb")

# Load data into DuckDB
con.execute(
    "CREATE OR REPLACE TABLE company_person_records AS "
    "SELECT * FROM company_person_records"
)


print("\n--- PHASE 1: COMPANY LINKAGE ---")
print("Company records to be linked:")
con.table("company_person_records").show()

# STEP 1: Find duplicate client_ids


# Configure company linkage
# We match on person name because if we have duplicate client_ids,
# it's likely that they may share the same contact
# Note though, at this stage the entity is client not a person
company_settings = SettingsCreator(
    link_type="dedupe_only",
    unique_id_column_name="unique_id",
    probability_two_random_records_match=0.001,
    comparisons=[
        cl.ExactMatch("client_id"),
        cl.JaroWinklerAtThresholds("person_firstname"),
        cl.JaroWinklerAtThresholds("person_surname"),
        cl.JaroWinklerAtThresholds("company_name"),
        cl.ExactMatch("postcode"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("postcode"),
        block_on("company_name"),
    ],
    retain_matching_columns=True,
)

db_api = DuckDBAPI(connection=con)
company_records_sdf = db_api.register("company_person_records")
company_linker = Linker(company_records_sdf, company_settings)
company_predictions = company_linker.inference.predict(threshold_match_probability=0.5)

print("\nCompany pairwise matches:")
company_predictions.as_duckdbpyrelation().show()

# Cluster companies
company_nodes = con.sql("SELECT DISTINCT client_id FROM company_person_records")
company_edges = con.sql(f"""
    SELECT
        client_id_l as n_1,
        client_id_r as n_2,
        match_probability
    FROM {company_predictions.physical_name}
""")

# Perform company clustering
company_clusters = cluster_pairwise_predictions_at_threshold(
    company_nodes,
    company_edges,
    node_id_column_name="client_id",
    edge_id_column_name_left="n_1",
    edge_id_column_name_right="n_2",
    db_api=db_api,
    threshold_match_probability=0.5,
)

# Add company cluster IDs to original records
company_clusters_ddb = company_clusters.as_duckdbpyrelation()
con.register("company_clusters_ddb", company_clusters_ddb)


sql = """
CREATE TABLE records_with_company_cluster AS
SELECT cr.*,
       cc.cluster_id as company_cluster_id
FROM company_person_records cr
LEFT JOIN company_clusters_ddb cc
ON cr.client_id = cc.client_id
"""
con.execute(sql)
print("Records with company cluster:")
con.table("records_with_company_cluster").show()

# Not needed, just to see what's happening
print("\nCompany clustering results:")
con.sql("""
SELECT
    company_cluster_id,
    array_agg(DISTINCT client_id) as client_ids,
    array_agg(DISTINCT company_name) as company_names
FROM records_with_company_cluster
GROUP BY company_cluster_id
""").show()

print("\n--- PHASE 2: PERSON LINKAGE WITHIN COMPANIES ---")
print("Now linking persons, but only within their company clusters")

# STEP 2: Link persons within company clusters
# Create a new connection to isolate this step
con2 = duckdb.connect()
con2.sql("attach 'nested_linkage.ddb' as linkage_db")
con2.execute(
    "create table records_with_company_cluster as select * from linkage_db.records_with_company_cluster"
)
db_api2 = DuckDBAPI(connection=con2)

# Configure person linkage within company clusters
# Simple linking model just distinguishes between people within a client_id
# There shouldn't be many so this model can be straightforward
person_settings = SettingsCreator(
    link_type="dedupe_only",
    probability_two_random_records_match=0.01,
    comparisons=[
        cl.JaroWinklerAtThresholds("person_firstname"),
        cl.JaroWinklerAtThresholds("person_surname"),
    ],
    blocking_rules_to_generate_predictions=[
        # Critical: Block on company_cluster_id to only compare within company
        block_on("company_cluster_id"),
    ],
    retain_matching_columns=True,
)

person_records_sdf = db_api2.register("records_with_company_cluster")
person_linker = Linker(person_records_sdf, person_settings)
person_predictions = person_linker.inference.predict(threshold_match_probability=0.5)

print("\nPerson pairwise matches (within company clusters):")
person_predictions.as_duckdbpyrelation().show(max_width=1000)

person_clusters = person_linker.clustering.cluster_pairwise_predictions_at_threshold(
    person_predictions, threshold_match_probability=0.5
)

person_clusters.as_duckdbpyrelation().sort("cluster_id").show(max_width=1000)



# %% [markdown]
# ## Comparing a list of values with fuzzy matching and term frequency adjustments
#
# See [here](https://github.com/moj-analytical-services/splink/discussions/2721) for a description of this approach
#
#

# %%
import duckdb

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator

con = duckdb.connect(database=":memory:")

left_records = [
    {
        "unique_id": 1,
        "primary_forename": "Alisha",
        "all_forenames": ["Alisha", "Alisha Louise", "Ali"],
    },
    {
        "unique_id": 2,
        "primary_forename": "Michael",
        "all_forenames": ["Michael", "Mike"],
    },
]

right_records = [
    {"unique_id": 1, "primary_forename": "Alisha", "all_forenames": ["Alisha", "Ali"]},
    {"unique_id": 3, "primary_forename": "Alysha", "all_forenames": ["Alysha"]},
    {"unique_id": 9, "primary_forename": "Michelle", "all_forenames": ["Michelle"]},
]


def make_table(name, recs):
    con.execute(f"drop table if exists {name}")
    con.execute(
        f"""
        create table {name} (
            unique_id integer,
            primary_forename varchar,
            all_forenames varchar[]
        )
    """
    )
    for r in recs:
        arr = "[" + ", ".join(f"'{v}'" for v in r["all_forenames"]) + "]"
        con.execute(
            f"""
            insert into {name} values
                ({r["unique_id"]}, '{r["primary_forename"]}', {arr})
        """
        )


make_table("df_left", left_records)
make_table("df_right", right_records)
con.table("df_left").show()
con.table("df_right").show()


forename_comparison = cl.CustomComparison(
    output_column_name="forename",
    comparison_levels=[
        cll.NullLevel("primary_forename"),
        # 1. exact with term-frequency adjustment
        cll.ExactMatchLevel("primary_forename", term_frequency_adjustments=True),
        # 2. any overlap between arrays
        cll.ArrayIntersectLevel("all_forenames", min_intersection=1),
        # 3. tight fuzzy
        cll.JaroWinklerLevel("primary_forename", distance_threshold=0.9),
        # 4. looser fuzzy
        cll.JaroWinklerLevel("primary_forename", distance_threshold=0.7),
        # 5. fuzzy anywhere in arrays
        cll.PairwiseStringDistanceFunctionLevel(
            col_name="all_forenames",
            distance_function_name="jaro_winkler",
            distance_threshold=0.85,
        ),
        cll.ElseLevel(),
    ],
    comparison_description="Forename comparison combining exact, array overlap and fuzzy logic",
)


settings = SettingsCreator(
    link_type="link_only",
    unique_id_column_name="unique_id",
    blocking_rules_to_generate_predictions=[
        # create all comparisons for demo
        "1=1"
    ],
    comparisons=[forename_comparison],
    retain_intermediate_calculation_columns=True,
    retain_matching_columns=True,
)
db_api_linker = DuckDBAPI(con)
df_left_sdf = db_api_linker.register("df_left")
df_right_sdf = db_api_linker.register("df_right")
linker = Linker(
    [df_left_sdf, df_right_sdf],
    settings,
)

# Skip training for demo purposes, just demonstrate that predict() works

df_predict = linker.inference.predict()

df_predict.as_duckdbpyrelation()
