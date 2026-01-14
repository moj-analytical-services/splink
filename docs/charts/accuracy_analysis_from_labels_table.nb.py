# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: base
#     language: python
#     name: python3
# ---

# %% [markdown]
# # `accuracy_analysis_from_labels_table`
#
#

# %% tags=["hide_input"]
chart

# %% [markdown]
# !!! info "At a glance"
#     **Useful for:** Selecting an optimal match weight threshold for generating linked clusters.
#
#     **API Documentation:** [accuracy_chart_from_labels_table()](../api_docs/evaluation.md#splink.internals.linker_components.evaluation.LinkerEvalution.accuracy_analysis_from_labels_table)
#
#     **What is needed to generate the chart?** A `linker` with some data and a corresponding labelled dataset

# %% [markdown]
# ### What the chart shows
#
# For a given match weight threshold, a record pair with a score above this threshold will be labelled a match and below the threshold will be labelled a non-match. For all possible match weight thresholds, this chart shows various accuracy metrics comparing the Splink scores against clerical labels. 
#
# **Precision** and **recall** are shown by default, but various additional metrics can be added: specificity, negative predictive value (NPV), accuracy, $F_1$, $F_2$, $F_{0.5}$, $P_4$ and $\phi$ (Matthews correlation coefficient).

# %% [markdown]
# ### How to interpret the chart
#
# **Precision** can be maximised by **increasing** the match threshold (reducing false positives).
#
# **Recall** can be maximised by **decreasing** the match threshold (reducing false negatives). 
#
# Additional metrics can be used to find the optimal compromise between these two, looking for the threshold at which peak accuracy is achieved. 
#
# !!! info "Confusion matrix"
#
#     See [threshold_selection_tool_from_labels_table](threshold_selection_tool_from_labels_table.ipynb) for a more complete visualisation of the impact of match threshold on false positives and false negatives, with reference to the confusion matrix.

# %% [markdown]
# ### Actions to take as a result of the chart
#
# Having identified an optimal match weight threshold, this can be applied when generating linked clusters using [cluster_pairwise_predictions_at_thresholds()](../api_docs/clustering.md#splink.clustering.cluster_pairwise_predictions_at_threshold).

# %% [markdown]
# ## Worked Example

# %% tags=["hide_output"]
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets
from splink.datasets import splink_dataset_labels

db_api = DuckDBAPI()

df = splink_datasets.fake_1000

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.JaroWinklerAtThresholds("first_name", [0.9, 0.7]),
        cl.JaroAtThresholds("surname", [0.9, 0.7]),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=True,
            datetime_metrics=["year", "month"],
            datetime_thresholds=[1, 1],
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("substr(first_name,1,1)"),
        block_on("substr(surname, 1,1)"),
    ],
)

linker = Linker(df, settings, db_api)

linker.training.estimate_probability_two_random_records_match(
    [block_on("first_name", "surname")], recall=0.7
)
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

blocking_rule_for_training = block_on("first_name", "surname")

linker.training.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training
)

blocking_rule_for_training = block_on("dob")
linker.training.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training
)


df_labels = splink_dataset_labels.fake_1000_labels
labels_table = linker.table_management.register_labels_table(df_labels)

chart = linker.evaluation.accuracy_analysis_from_labels_table(
    labels_table, output_type="accuracy", add_metrics=["f1"]
)

# %% [markdown]
# Note that you can also produce a ROC chart, a precision recall chart, or get the results as a table:

# %%
linker.evaluation.accuracy_analysis_from_labels_table(
    labels_table, output_type="roc", add_metrics=["f1"]
)

# %%
linker.evaluation.accuracy_analysis_from_labels_table(
    labels_table, output_type="precision_recall", add_metrics=["f1"]
)

# %%
linker.evaluation.accuracy_analysis_from_labels_table(
    labels_table, output_type="table", add_metrics=["f1"]
).as_pandas_dataframe()

# %% [markdown]
#
