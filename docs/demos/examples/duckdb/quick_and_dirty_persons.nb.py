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
# ## Historical people: Quick and dirty
#
# This example shows how to get some initial record linkage results as quickly as possible.
#
# There are many ways to improve the accuracy of this model. But this may be a good place to start if you just want to give Splink a try and see what it's capable of.
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/quick_and_dirty_persons.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
from splink.datasets import splink_datasets

df = splink_datasets.historical_50k
df.head(5)

# %%
from splink import block_on, SettingsCreator
import splink.comparison_library as cl


settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("full_name"),
        block_on("substr(full_name,1,6)", "dob", "birth_place"),
        block_on("dob", "birth_place"),
        block_on("postcode_fake"),
    ],
    comparisons=[
        cl.ForenameSurnameComparison(
            "first_name",
            "surname",
            forename_surname_concat_col_name="first_and_surname",
        ),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=True,
        ),
        cl.LevenshteinAtThresholds("postcode_fake", 2),
        cl.JaroWinklerAtThresholds("birth_place", 0.9).configure(
            term_frequency_adjustments=True
        ),
        cl.ExactMatch("occupation").configure(term_frequency_adjustments=True),
    ],
)

# %%
from splink import Linker, DuckDBAPI


db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings, set_up_basic_logging=False)
deterministic_rules = [
    "l.full_name = r.full_name",
    "l.postcode_fake = r.postcode_fake and l.dob = r.dob",
]

linker.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.6
)

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=2e6)

# %%
results = linker.inference.predict(threshold_match_probability=0.9)

# %%
results.as_pandas_dataframe(limit=5)
