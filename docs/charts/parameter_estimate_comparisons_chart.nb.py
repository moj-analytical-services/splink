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
# # `parameter_estimate_comparisons_chart`

# %% tags=["hide_input"]
chart

# %% [markdown]
#
# !!! info "At a glance"
#     **Useful for:** Looking at the m and u value estimates across multiple Splink model training sessions.
#
#     **API Documentation:** [parameter_estimate_comparisons_chart()](../api_docs/visualisations.md#splink.internals.linker_components.visualisations.LinkerVisualisations.parameter_estimate_comparisons_chart)
#
#     **What is needed to generate the chart?** A trained Splink model.

# %% [markdown]
# ## Related Charts
#
# ::cards::
# [
#     {
#     "title": "`m u parameters chart`",
#     "image": "./img/m_u_parameters_chart.png",
#     "url": "./m_u_parameters_chart.ipynb"
#     },
#     {
#     "title": "`match weights chart`",
#     "image": "./img/match_weights_chart.png",
#     "url": "./match_weights_chart.ipynb"
#     },
# ]
# ::/cards::

# %% [markdown]
# ## Worked Example

# %% tags=["hide_output"]
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

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
        block_on("first_name"),
        block_on("surname"),
    ],
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

blocking_rule_for_training = block_on("email")
linker.training.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training
)

chart = linker.visualisations.parameter_estimate_comparisons_chart()
chart


# %%
