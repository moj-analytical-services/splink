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
# # `cluster_studio_dashboard`
#

# %% tags=["hide_input"]
from IPython.display import IFrame
IFrame(src="./img/cluster_studio.html", width="100%", height=1000)

# %% [markdown]
#
# !!! info "At a glance"
#
#     **API Documentation:** [cluster_studio_dashboard()](../api_docs/visualisations.md#splink.internals.linker_components.visualisations.LinkerVisualisations.cluster_studio_dashboard)

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
        block_on("substr(first_name,1,1)"),
        block_on("substr(surname, 1,1)"),
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
df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predictions, threshold_match_probability=0.5
)

linker.visualisations.cluster_studio_dashboard(
    df_predictions, df_clusters, "img/cluster_studio.html",
    sampling_method="by_cluster_size", overwrite=True
)

# You can view the scv.html file in your browser, or inline in a notebook as follows
from IPython.display import IFrame
IFrame(src="./img/cluster_studio.html", width="100%", height=1200)


# %% [markdown]
# ### What the chart shows
#
# See [here](https://youtu.be/msz3T741KQI?si=1VCK48bwENFcUyQS&t=2741) for a video explanation of the chart.
