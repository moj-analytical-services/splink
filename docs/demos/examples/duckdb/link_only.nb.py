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
# ## Linking without deduplication
#
# A simple record linkage model using the `link_only` [link type](https://moj-analytical-services.github.io/splink/settings_dict_guide.html#link_type).
#
# With `link_only`, only between-dataset record comparisons are generated. No within-dataset record comparisons are created, meaning that the model does not attempt to find within-dataset duplicates.
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/link_only.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
from splink import splink_datasets
from splink.internals.misc import show
import duckdb

df = splink_datasets.fake_1000

# Split a simple dataset into two, separate datasets which can be linked together.
rel = duckdb.sql("select * from df")
df_l = rel.filter("hash(unique_id) % 2 = 0").arrow().read_all()
df_r = rel.filter("hash(unique_id) % 2 = 1").arrow().read_all()

show(df_r, rows=2)
show(df_l, rows=2)

# %%
import splink.comparison_library as cl

from splink import DuckDBAPI, Linker, SettingsCreator, block_on

settings = SettingsCreator(
    link_type="link_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    comparisons=[
        cl.NameComparison(
            "first_name",
        ),
        cl.NameComparison("surname"),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=False,
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
)

db_api = DuckDBAPI()
df_l_sdf = db_api.register(df_l, dataset_display_name="df_left")
df_r_sdf = db_api.register(df_r, dataset_display_name="df_right")
linker = Linker([df_l_sdf, df_r_sdf], settings)

# %%
from splink.exploratory import completeness_chart

db_api = DuckDBAPI()
df_l_sdf = db_api.register(df_l, dataset_display_name="df_left")
df_r_sdf = db_api.register(df_r, dataset_display_name="df_right")
completeness_chart(
    [df_l_sdf, df_r_sdf],
    cols=["first_name", "surname", "dob", "city", "email"],
    table_names_for_chart=["df_left", "df_right"],
)

# %%

deterministic_rules = [
    "l.first_name = r.first_name and levenshtein(r.dob::VARCHAR, l.dob::VARCHAR) <= 1",
    "l.surname = r.surname and levenshtein(r.dob::VARCHAR, l.dob::VARCHAR) <= 1",
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    block_on("email"),
]


linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=1e6, seed=1)

# %%
session_dob = linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))
session_email = linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("email")
)
session_first_name = linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("first_name")
)

# %%
results = linker.inference.predict(threshold_match_probability=0.9)

# %%
results.as_duckdbpyrelation().limit(5).show(max_width=10000)


