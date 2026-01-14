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
# # `waterfall_chart`
#

# %% tags=["hide_input"]
chart

# %% [markdown]
#
# !!! info "At a glance"
#
#     **Useful for:** Looking at the breakdown of the match weight for a pair of records.
#
#     **API Documentation:** [waterfall_chart()](../api_docs//visualisations.md#splink.internals.linker_components.visualisations.LinkerVisualisations.waterfall_chart)
#
#     **What is needed to generate the chart?** A trained Splink model

# %% [markdown]
# ### What the chart shows
#
# The `waterfall_chart` shows the amount of evidence of a match that is provided by each comparison for a pair of records. Each bar represents a comparison and the corresponding amount of evidence (i.e. match weight) of a match for the pair of values displayed above the bar.
#
# ??? note "What the chart tooltip shows"
#
#     ![](./img/waterfall_chart_tooltip.png)
#
#     The tooltip contains information based on the bar that the user is hovering over, including:
#
#     - The comparison column (or columns)
#     - The column values from the pair of records being compared
#     - The comparison level as a label, SQL statement and the corresponding comparison vector value
#     - The bayes factor (i.e. how many times more likely is a match based on this evidence)
#     - The match weight for the comparison level
#     - The cumulative match probability from the chosen comparison and all of the previous comparisons.

# %% [markdown]
# <hr>

# %% [markdown]
# ### How to interpret the chart
#
# The first bar (labelled "Prior") is the match weight if no additional knowledge of features is taken into account, and can be thought of as similar to the y-intercept in a simple regression.
#
# Each subsequent bar shows the match weight for a comparison. These bars can be positive or negative depending on whether the given comparison gives positive or negative evidence for the two records being a match.
#
# Additional bars are added for comparisons with term frequency adjustments. For example, the chart above has term frequency adjustments for `first_name` so there is an extra `tf_first_name` bar showing how the frequency of a given name impacts the amount of evidence for the two records being a match.
#
# The final bar represents total match weight for the pair of records. This match weight can also be translated into a final match probablility, and the corresponding match probability is shown on the right axis (note the logarithmic scale).

# %% [markdown]
# <hr>

# %% [markdown]
# ### Actions to take as a result of the chart
#
# This chart is useful for spot checking pairs of records to see if the Splink model is behaving as expected.
#
# If a pair of records look like they are incorrectly being assigned as a match/non-match, it is a sign that the Splink model is not working optimally. If this is the case, it is worth revisiting the model training step. 
#
# Some common scenarios include:
#
# - If a comparison isn't capturing a specific edge case (e.g. fuzzy match), add a comparison level to capture this case and retrain the model.
#
# - If the match weight for a comparison is looking unusual, refer to the [`match_weights_chart`](./match_weights_chart.ipynb) to see the match weight in context with the rest of the comparison levels within that comparison. If it is still looking unusual, you can dig deeper with the [`parameter_estimate_comparisons_chart`](./parameter_estimate_comparisons_chart.ipynb) to see if the model training runs are consistent. If there is a lot of variation between model training sessions, this can suggest some instability in the model. In this case, try some different model training rules and/or comparison levels.
#
# - If the "Prior" match weight is too small or large compared to the match weight provided by the comparisons, try some different determininstic rules and recall inputs to the [`estimate_probability_two_records_match` function](../api_docs/training.md).
#
# - If you are working with a model with term frequency adjustments and want to dig deeper into the impact of term frequency on the model as a whole (i.e. not just for a single pairwise comparison), check out the [`tf_adjustment_chart`](./tf_adjustment_chart.ipynb).
#

# %% [markdown]
# ## Worked Example

# %% tags=["hide_output"]
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

df = splink_datasets.fake_1000

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.NameComparison("first_name").configure(term_frequency_adjustments=True),
        cl.NameComparison("surname"),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=True,
            datetime_metrics=["year", "month"],
            datetime_thresholds=[1, 1],
        ),
        cl.ExactMatch("city"),
        cl.EmailComparison("email", include_username_fuzzy_level=False),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    retain_intermediate_calculation_columns=True,
    retain_matching_columns=True,
)

linker = Linker(df, settings, DuckDBAPI())
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

blocking_rule_for_training = block_on("first_name", "surname")
linker.training.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training
)

blocking_rule_for_training = block_on("dob")
linker.training.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training
)

df_predictions = linker.inference.predict(threshold_match_probability=0.2)
records_to_view = df_predictions.as_record_dict(limit=5)

chart = linker.visualisations.waterfall_chart(records_to_view, filter_nulls=False)
chart
