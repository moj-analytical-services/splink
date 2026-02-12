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
# ## Deduplicating the febrl3 dataset
#
# See A.2 [here](https://arxiv.org/pdf/2008.04443.pdf) and [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html) for the source of this data
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/febrl3.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %% tags=["hide_output"]
from splink.datasets import splink_datasets

df = splink_datasets.febrl3

# %%
df = df.rename(columns=lambda x: x.strip())

df["cluster"] = df["rec_id"].apply(lambda x: "-".join(x.split("-")[:2]))

df["date_of_birth"] = df["date_of_birth"].astype(str).str.strip()
df["soc_sec_id"] = df["soc_sec_id"].astype(str).str.strip()

df.head(2)

# %%
df["date_of_birth"] = df["date_of_birth"].astype(str).str.strip()
df["soc_sec_id"] = df["soc_sec_id"].astype(str).str.strip()

# %%
df["date_of_birth"] = df["date_of_birth"].astype(str).str.strip()
df["soc_sec_id"] = df["soc_sec_id"].astype(str).str.strip()

# %%
from splink import DuckDBAPI, Linker, SettingsCreator

# TODO:  Allow missingness to be analysed without a linker
settings = SettingsCreator(
    unique_id_column_name="rec_id",
    link_type="dedupe_only",
)

db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)

# %% [markdown]
# It's usually a good idea to perform exploratory analysis on your data so you understand what's in each column and how often it's missing:
#

# %%
from splink.exploratory import completeness_chart

completeness_chart(df_sdf)

# %%
from splink.exploratory import profile_columns

profile_columns(df_sdf, column_expressions=["given_name", "surname"])

# %%
from splink import DuckDBAPI, block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

blocking_rules = [
    block_on("soc_sec_id"),
    block_on("given_name"),
    block_on("surname"),
    block_on("date_of_birth"),
    block_on("postcode"),
]


cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    df_sdf,
    blocking_rules=blocking_rules,
    link_type="dedupe_only",
    unique_id_column_name="rec_id",
)

# %%
import splink.comparison_library as cl

from splink import Linker

settings = SettingsCreator(
    unique_id_column_name="rec_id",
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        cl.NameComparison("given_name"),
        cl.NameComparison("surname"),
        cl.DateOfBirthComparison(
            "date_of_birth",
            input_is_string=True,
            datetime_format="%Y%m%d",
        ),
        cl.DamerauLevenshteinAtThresholds("soc_sec_id", [2]),
        cl.ExactMatch("street_number").configure(term_frequency_adjustments=True),
        cl.ExactMatch("postcode").configure(term_frequency_adjustments=True),
    ],
    retain_intermediate_calculation_columns=True,
)

db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)

# %%
from splink import block_on

deterministic_rules = [
    block_on("soc_sec_id"),
    block_on("given_name", "surname", "date_of_birth"),
    "l.given_name = r.surname and l.surname = r.given_name and l.date_of_birth = r.date_of_birth",
]

linker.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.9
)

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

# %%
em_blocking_rule_1 = block_on("date_of_birth")
session_dob = linker.training.estimate_parameters_using_expectation_maximisation(
    em_blocking_rule_1
)

# %%
em_blocking_rule_2 = block_on("postcode")
session_postcode = linker.training.estimate_parameters_using_expectation_maximisation(
    em_blocking_rule_2
)

# %%
linker.visualisations.match_weights_chart()

# %%
results = linker.inference.predict(threshold_match_probability=0.2)

# %%
linker.evaluation.accuracy_analysis_from_labels_column(
    "cluster", match_weight_round_to_nearest=0.1, output_type="accuracy"
)

# %%
pred_errors_df = linker.evaluation.prediction_errors_from_labels_column(
    "cluster"
).as_pandas_dataframe()
len(pred_errors_df)
pred_errors_df.head()

# %% [markdown]
# The following chart seems to suggest that, where the model is making errors, it's because the data is corrupted beyond recognition and no reasonable linkage model could find these matches

# %%
records = linker.evaluation.prediction_errors_from_labels_column(
    "cluster"
).as_record_dict(limit=10)
linker.visualisations.waterfall_chart(records)
