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
# ## Linking a dataset of real historical persons
#
# In this example, we deduplicate a more realistic dataset. The data is based on historical persons scraped from wikidata. Duplicate records are introduced with a variety of errors introduced.
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/deduplicate_50k_synthetic.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input", "hide_output"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %% tags=["hide_output"]
from splink import splink_datasets

df = splink_datasets.historical_50k

# %%
df.head()

# %%
from splink import DuckDBAPI
from splink.exploratory import profile_columns

db_api = DuckDBAPI()
df_sdf = db_api.register(df)
profile_columns(df_sdf, column_expressions=["first_name", "substr(surname,1,2)"])

# %%
from splink import DuckDBAPI, block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

blocking_rules = [
    block_on("substr(first_name,1,3)", "substr(surname,1,4)"),
    block_on("surname", "dob"),
    block_on("first_name", "dob"),
    block_on("postcode_fake", "first_name"),
    block_on("postcode_fake", "surname"),
    block_on("dob", "birth_place"),
    block_on("substr(postcode_fake,1,3)", "dob"),
    block_on("substr(postcode_fake,1,3)", "first_name"),
    block_on("substr(postcode_fake,1,3)", "surname"),
    block_on("substr(first_name,1,2)", "substr(surname,1,2)", "substr(dob,1,4)"),
]


cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    df_sdf,
    blocking_rules=blocking_rules,
    link_type="dedupe_only",
)

# %%
import splink.comparison_library as cl

from splink import Linker, SettingsCreator

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        cl.ForenameSurnameComparison(
            "first_name",
            "surname",
            forename_surname_concat_col_name="first_name_surname_concat",
        ),
        cl.DateOfBirthComparison(
            "dob", input_is_string=True
        ),
        cl.PostcodeComparison("postcode_fake"),
        cl.ExactMatch("birth_place").configure(term_frequency_adjustments=True),
        cl.ExactMatch("occupation").configure(term_frequency_adjustments=True),
    ],
    retain_intermediate_calculation_columns=True,
)
# Needed to apply term frequencies to first+surname comparison
df["first_name_surname_concat"] = df["first_name"] + " " + df["surname"]

df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)

# %%
linker.training.estimate_probability_two_random_records_match(
    [
        block_on("first_name", "surname", "dob"),
        block_on("substr(first_name,1,2)", "surname", "substr(postcode_fake,1,2)"),
        block_on("dob", "postcode_fake"),
    ],
    recall=0.6,
)

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=5e6)

# %%
training_blocking_rule = block_on("first_name", "surname")
training_session_names = (
    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule, estimate_without_term_frequencies=True
    )
)

# %%
training_blocking_rule = block_on("dob")
training_session_dob = (
    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule, estimate_without_term_frequencies=True
    )
)

# %% [markdown]
# The final match weights can be viewed in the match weights chart:
#

# %%
linker.visualisations.match_weights_chart()

# %%
linker.evaluation.unlinkables_chart()

# %%
df_predict = linker.inference.predict()
df_e = df_predict.as_pandas_dataframe(limit=5)
df_e

# %% [markdown]
# You can also view rows in this dataset as a waterfall chart as follows:
#

# %%
records_to_plot = df_e.to_dict(orient="records")
linker.visualisations.waterfall_chart(records_to_plot, filter_nulls=False)

# %%
clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predict, threshold_match_probability=0.95
)

# %%
from IPython.display import IFrame

linker.visualisations.cluster_studio_dashboard(
    df_predict,
    clusters,
    "dashboards/50k_cluster.html",
    sampling_method="by_cluster_size",
    overwrite=True,
)


IFrame(src="./dashboards/50k_cluster.html", width="100%", height=1200)

# %%
linker.evaluation.accuracy_analysis_from_labels_column(
    "cluster", output_type="accuracy", match_weight_round_to_nearest=0.02
)

# %%
records = linker.evaluation.prediction_errors_from_labels_column(
    "cluster",
    threshold_match_probability=0.999,
    include_false_negatives=False,
    include_false_positives=True,
).as_record_dict()
linker.visualisations.waterfall_chart(records)

# %%
# Some of the false negatives will be because they weren't detected by the blocking rules
records = linker.evaluation.prediction_errors_from_labels_column(
    "cluster",
    threshold_match_probability=0.5,
    include_false_negatives=True,
    include_false_positives=False,
).as_record_dict(limit=50)

linker.visualisations.waterfall_chart(records)
