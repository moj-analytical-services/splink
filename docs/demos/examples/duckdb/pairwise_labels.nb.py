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
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/pairwise_labels.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>

# %% [markdown]
# ## Estimating m from a sample of pairwise labels
#

# %% [markdown]
#
# In this example, we estimate the m probabilities of the model from a table containing pairwise record comparisons which we know are 'true' matches. For example, these may be the result of work by a clerical team who have manually labelled a sample of matches.
#
# The table must be in the following format:
#
# | source_dataset_l | unique_id_l | source_dataset_r | unique_id_r |
# | ---------------- | ----------- | ---------------- | ----------- |
# | df_1             | 1           | df_2             | 2           |
# | df_1             | 1           | df_2             | 3           |
#
# It is assumed that every record in the table represents a certain match.
#
# Note that the column names above are the defaults. They should correspond to the values you've set for [`unique_id_column_name`](https://moj-analytical-services.github.io/splink/settings_dict_guide.html#unique_id_column_name) and [`source_dataset_column_name`](https://moj-analytical-services.github.io/splink/settings_dict_guide.html#source_dataset_column_name), if you've chosen custom values.
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
from splink.datasets import splink_dataset_labels

pairwise_labels = splink_dataset_labels.fake_1000_labels

# Choose labels indicating a match
pairwise_labels = pairwise_labels[pairwise_labels["clerical_match_score"] == 1]
pairwise_labels

# %% [markdown]
# We now proceed to estimate the Fellegi Sunter model:
#

# %%
from splink import splink_datasets

df = splink_datasets.fake_1000
df.head(2)

# %%
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    comparisons=[
        cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=False,
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    retain_intermediate_calculation_columns=True,
)

# %%
db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings, set_up_basic_logging=False)
deterministic_rules = [
    "l.first_name = r.first_name and levenshtein(r.dob::VARCHAR, l.dob::VARCHAR) <= 1",
    "l.surname = r.surname and levenshtein(r.dob::VARCHAR, l.dob::VARCHAR) <= 1",
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    "l.email = r.email",
]

linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

# %%
# Register the pairwise labels table with the database, and then use it to estimate the m values
labels_df = linker.table_management.register_labels_table(pairwise_labels, overwrite=True)
linker.training.estimate_m_from_pairwise_labels(labels_df)


# If the labels table already existing in the dataset you could run
# linker.training.estimate_m_from_pairwise_labels("labels_tablename_here")

# %%
training_blocking_rule = block_on("first_name")
linker.training.estimate_parameters_using_expectation_maximisation(training_blocking_rule)

# %%
linker.visualisations.parameter_estimate_comparisons_chart()

# %%
linker.visualisations.match_weights_chart()
