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
# Note, as explained in the [backends topic guide](../../../topic_guides/splink_fundamentals/backends/backends.md#sqlite), SQLite does not natively support string fuzzy matching functions such as `damareau-levenshtein` and `jaro-winkler` (as used in this example). Instead, these have been imported as python User Defined Functions (UDFs). One drawback of python UDFs is that they are considerably slower than native-SQL comparisons. As such, if you are hitting issues with large run times, consider switching to DuckDB (or some other backend).
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/sqlite/deduplicate_50k_synthetic.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>

# %%
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink
# # !pip install rapidfuzz

# %%
import pandas as pd

from splink import splink_datasets

pd.options.display.max_rows = 1000
# reduce size of dataset to make things run faster
df = splink_datasets.historical_50k.sample(5000)

# %%
from splink.backends.sqlite import SQLiteAPI
from splink.exploratory import profile_columns

db_api = SQLiteAPI()
df_sdf = db_api.register(df)
profile_columns(
    df_sdf, column_expressions=["first_name", "postcode_fake", "substr(dob, 1,4)"]
)

# %%
from splink import block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

blocking_rules =  [block_on("first_name", "surname"),
        block_on("surname", "dob"),
        block_on("first_name", "dob"),
        block_on("postcode_fake", "first_name")]



cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    df_sdf,
    blocking_rules=blocking_rules,
    link_type="dedupe_only"
)

# %%
import splink.comparison_library as cl
from splink import Linker

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        block_on("first_name", "surname"),
        block_on("surname", "dob"),
        block_on("first_name", "dob"),
        block_on("postcode_fake", "first_name"),

    ],
    "comparisons": [
        cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.DamerauLevenshteinAtThresholds("dob", [1, 2]).configure(
            term_frequency_adjustments=True
        ),
        cl.DamerauLevenshteinAtThresholds("postcode_fake", [1, 2]),
        cl.ExactMatch("birth_place").configure(term_frequency_adjustments=True),
        cl.ExactMatch(
            "occupation",
        ).configure(term_frequency_adjustments=True),
    ],
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
    "max_iterations": 10,
    "em_convergence": 0.01,
}

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
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

# %%
training_blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
training_session_names = linker.training.estimate_parameters_using_expectation_maximisation(
    training_blocking_rule, estimate_without_term_frequencies=True
)

# %%
training_blocking_rule = "l.dob = r.dob"
training_session_dob = linker.training.estimate_parameters_using_expectation_maximisation(
    training_blocking_rule, estimate_without_term_frequencies=True
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
linker.visualisations.cluster_studio_dashboard(
    df_predict,
    clusters,
    "dashboards/50k_cluster.html",
    sampling_method="by_cluster_size",
    overwrite=True,
)

from IPython.display import IFrame

IFrame(src="./dashboards/50k_cluster.html", width="100%", height=1200)

# %%
linker.evaluation.accuracy_analysis_from_labels_column(
    "cluster", output_type="roc", match_weight_round_to_nearest=0.02
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
