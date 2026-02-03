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
# ## Linking the febrl4 datasets
#
# See A.2 [here](https://arxiv.org/pdf/2008.04443.pdf) and [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html) for the source of this data.
#
# It consists of two datasets, A and B, of 5000 records each, with each record in dataset A having a corresponding record in dataset B. The aim will be to capture as many of those 5000 true links as possible, with minimal false linkages.
#
# It is worth noting that we should not necessarily expect to capture _all_ links. There are some links that although we know they _do_ correspond to the same person, the data is so mismatched between them that we would not reasonably expect a model to link them, and indeed should a model do so may indicate that we have overengineered things using our knowledge of true links, which will not be a helpful reference in situations where we attempt to link unlabelled data, as will usually be the case.
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/febrl4.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %% [markdown]
# ### Exploring data and defining model
#

# %% [markdown]
# Firstly let's read in the data and have a little look at it
#

# %%
from IPython.display import display

from splink import splink_datasets

df_a = splink_datasets.febrl4a
df_b = splink_datasets.febrl4b


def prepare_data(data):
    data = data.rename(columns=lambda x: x.strip())
    data["cluster"] = data["rec_id"].apply(lambda x: "-".join(x.split("-")[:2]))
    data["date_of_birth"] = data["date_of_birth"].astype(str).str.strip()
    data["soc_sec_id"] = data["soc_sec_id"].astype(str).str.strip()
    data["postcode"] = data["postcode"].astype(str).str.strip()
    return data


dfs = [prepare_data(dataset) for dataset in [df_a, df_b]]

display(dfs[0].head(2))
display(dfs[1].head(2))

# %% [markdown]
# Next, to better understand which variables will prove useful in linking, we have a look at how populated each column is, as well as the distribution of unique values within each
#

# %%
from splink import DuckDBAPI, Linker, SettingsCreator

basic_settings = SettingsCreator(
    unique_id_column_name="rec_id",
    link_type="link_only",
    # NB as we are linking one-one, we know the probability that a random pair will be a match
    # hence we could set:
    # "probability_two_random_records_match": 1/5000,
    # however we will not specify this here, as we will use this as a check that
    # our estimation procedure returns something sensible
)

db_api = DuckDBAPI()
dfs_sdf = [db_api.register(df) for df in dfs]
linker = Linker(dfs_sdf, basic_settings)

# %% [markdown]
# It's usually a good idea to perform exploratory analysis on your data so you understand what's in each column and how often it's missing
#

# %%
from splink.exploratory import completeness_chart

db_api = DuckDBAPI()
dfs_sdf = [db_api.register(df) for df in dfs]
completeness_chart(dfs_sdf)

# %%
from splink.exploratory import profile_columns

db_api = DuckDBAPI()
dfs_sdf = [db_api.register(df) for df in dfs]
profile_columns(dfs_sdf, column_expressions=["given_name", "surname"])

# %% [markdown]
# Next let's come up with some candidate blocking rules, which define which record comparisons are generated, and have a look at how many comparisons each will generate.
#
# For blocking rules that we use in prediction, our aim is to have the union of all rules cover all true matches, whilst avoiding generating so many comparisons that it becomes computationally intractable - i.e. each true match should have at least _one_ of the following conditions holding.
#

# %%
from splink import DuckDBAPI, block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

blocking_rules = [
    block_on("given_name", "surname"),
    # A blocking rule can also be an aribtrary SQL expression
    "l.given_name = r.surname and l.surname = r.given_name",
    block_on("date_of_birth"),
    block_on("soc_sec_id"),
    block_on("state", "address_1"),
    block_on("street_number", "address_1"),
    block_on("postcode"),
]


db_api = DuckDBAPI()
dfs_sdf = [db_api.register(df) for df in dfs]
cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    dfs_sdf,
    blocking_rules=blocking_rules,
    link_type="link_only",
    unique_id_column_name="rec_id",
    source_dataset_column_name="source_dataset",
)

# %% [markdown]
# The broadest rule, having a matching postcode, unsurpisingly gives the largest number of comparisons.
# For this small dataset we still have a very manageable number, but if it was larger we might have needed to include a further `AND` condition with it to break the number of comparisons further.
#

# %% [markdown]
# Now we get the full settings by including the blocking rules, as well as deciding the actual comparisons we will be including in our model.
#
# We will define two models, each with a separate linker with different settings, so that we can compare performance. One will be a very basic model, whilst the other will include a lot more detail.
#

# %%
import splink.comparison_level_library as cll
import splink.comparison_library as cl


# the simple model only considers a few columns, and only two comparison levels for each
simple_model_settings = SettingsCreator(
    unique_id_column_name="rec_id",
    link_type="link_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        cl.ExactMatch("given_name").configure(term_frequency_adjustments=True),
        cl.ExactMatch("surname").configure(term_frequency_adjustments=True),
        cl.ExactMatch("street_number").configure(term_frequency_adjustments=True),
    ],
    retain_intermediate_calculation_columns=True,
)

# the detailed model considers more columns, using the information we saw in the exploratory phase
# we also include further comparison levels to account for typos and other differences
detailed_model_settings = SettingsCreator(
    unique_id_column_name="rec_id",
    link_type="link_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        cl.NameComparison("given_name").configure(term_frequency_adjustments=True),
        cl.NameComparison("surname").configure(term_frequency_adjustments=True),
        cl.DateOfBirthComparison(
            "date_of_birth",
            input_is_string=True,
            datetime_format="%Y%m%d",
            invalid_dates_as_null=True,
        ),
        cl.DamerauLevenshteinAtThresholds("soc_sec_id", [1, 2]),
        cl.ExactMatch("street_number").configure(term_frequency_adjustments=True),
        cl.DamerauLevenshteinAtThresholds("postcode", [1, 2]).configure(
            term_frequency_adjustments=True
        ),
        # we don't consider further location columns as they will be strongly correlated with postcode
    ],
    retain_intermediate_calculation_columns=True,
)


db_api = DuckDBAPI()
dfs_sdf = [db_api.register(df) for df in dfs]
linker_simple = Linker(dfs_sdf, simple_model_settings)
linker_detailed = Linker(dfs_sdf, detailed_model_settings)

# %% [markdown]
# ### Estimating model parameters
#

# %% [markdown]
# We need to furnish our models with parameter estimates so that we can generate results. We will focus on the detailed model, generating the values for the simple model at the end
#

# %% [markdown]
# We can instead estimate the probability two random records match, and compare with the known value of 1/5000 = 0.0002, to see how well our estimation procedure works.
#
# To do this we come up with some deterministic rules - the aim here is that we generate very few false positives (i.e. we expect that the majority of records with at least one of these conditions holding are true matches), whilst also capturing the majority of matches - our guess here is that these two rules should capture 80% of all matches.
#

# %%
deterministic_rules = [
    block_on("soc_sec_id"),
    block_on("given_name", "surname", "date_of_birth"),
]

linker_detailed.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.8
)

# %% [markdown]
# Even playing around with changing these deterministic rules, or the nominal recall leaves us with an answer which is pretty close to our known value
#

# %% [markdown]
# Next we estimate `u` and `m` values for each comparison, so that we can move to generating predictions
#

# %%
# We generally recommend setting max pairs higher (e.g. 1e7 or more)
# But this will run faster for the purpose of this demo
linker_detailed.training.estimate_u_using_random_sampling(max_pairs=1e6)

# %% [markdown]
# When training the `m` values using expectation maximisation, we need somre more blocking rules to reduce the total number of comparisons. For each rule, we want to ensure that we have neither proportionally too many matches, or too few.
#
# We must run this multiple times using different rules so that we can obtain estimates for all comparisons - if we block on e.g. `date_of_birth`, then we cannot compute the `m` values for the `date_of_birth` comparison, as we have only looked at records where these match.
#

# %%
session_dob = (
    linker_detailed.training.estimate_parameters_using_expectation_maximisation(
        block_on("date_of_birth"), estimate_without_term_frequencies=True
    )
)
session_pc = (
    linker_detailed.training.estimate_parameters_using_expectation_maximisation(
        block_on("postcode"), estimate_without_term_frequencies=True
    )
)

# %% [markdown]
# If we wish we can have a look at how our parameter estimates changes over these training sessions
#

# %%
session_dob.m_u_values_interactive_history_chart()

# %% [markdown]
# For variables that aren't used in the `m`-training blocking rules, we have two estimates --- one from each of the training sessions (see for example `street_number`). We can have a look at how the values compare between them, to ensure that we don't have drastically different values, which may be indicative of an issue.
#

# %%
linker_detailed.visualisations.parameter_estimate_comparisons_chart()

# %% [markdown]
# We repeat our parameter estimations for the simple model in much the same fashion
#

# %%
linker_simple.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.8
)
linker_simple.training.estimate_u_using_random_sampling(max_pairs=1e7)
session_ssid = (
    linker_simple.training.estimate_parameters_using_expectation_maximisation(
        block_on("given_name"), estimate_without_term_frequencies=True
    )
)
session_pc = linker_simple.training.estimate_parameters_using_expectation_maximisation(
    block_on("street_number"), estimate_without_term_frequencies=True
)
linker_simple.visualisations.parameter_estimate_comparisons_chart()

# %%
# import json
# we can have a look at the full settings if we wish, including the values of our estimated parameters:
# print(json.dumps(linker_detailed._settings_obj.as_dict(), indent=2))
# we can also get a handy summary of of the model in an easily readable format if we wish:
# print(linker_detailed._settings_obj.human_readable_description)
# (we suppress output here for brevity)

# %% [markdown]
# We can now visualise some of the details of our models. We can look at the match weights, which tell us the relative importance for/against a match for each of our comparsion levels.
#
# Comparing the two models will show the added benefit we get in the more detailed model --- what in the simple model is classed as 'all other comparisons' is instead broken down further, and we can see that the detail of how this is broken down in fact gives us quite a bit of useful information about the likelihood of a match.
#

# %%
linker_simple.visualisations.match_weights_chart()

# %%
linker_detailed.visualisations.match_weights_chart()

# %% [markdown]
# As well as the match weights, which give us an idea of the overall effect of each comparison level, we can also look at the individual `u` and `m` parameter estimates, which tells us about the prevalence of coincidences and mistakes (for further details/explanation about this see [this article](https://www.robinlinacre.com/maths_of_fellegi_sunter/)). We might want to revise aspects of our model based on the information we ascertain here.
#
# Note however that some of these values are very small, which is why the match weight chart is often more useful for getting a decent picture of things.
#

# %%
# linker_simple.m_u_parameters_chart()
linker_detailed.visualisations.m_u_parameters_chart()

# %% [markdown]
# It is also useful to have a look at unlinkable records - these are records which do not contain enough information to be linked at some match probability threshold. We can figure this out be seeing whether records are able to be matched with themselves.
#
# This is of course relative to the information we have put into the model - we see that in our simple model, at a 99% match threshold nearly 10% of records are unlinkable, as we have not included enough information in the model for distinct records to be adequately distinguished; this is not an issue in our more detailed model.
#

# %%
linker_simple.evaluation.unlinkables_chart()

# %%
linker_detailed.evaluation.unlinkables_chart()

# %% [markdown]
# Our simple model doesn't do _terribly_, but suffers if we want to have a high match probability --- to be 99% (match weight ~7) certain of matches we have ~10% of records that we will be unable to link.
#
# Our detailed model, however, has enough nuance that we can at least self-link records.
#

# %% [markdown]
# ### Predictions
#
# Now that we have had a look into the details of the models, we will focus on only our more detailed model, which should be able to capture more of the genuine links in our data
#

# %%
predictions = linker_detailed.inference.predict(threshold_match_probability=0.2)
df_predictions = predictions.as_pandas_dataframe()
df_predictions.head(5)

# %% [markdown]
# We can see how our model performs at different probability thresholds, with a couple of options depending on the space we wish to view things
#

# %%
linker_detailed.evaluation.accuracy_analysis_from_labels_column(
    "cluster", output_type="accuracy"
)

# %% [markdown]
# and we can easily see how many individuals we identify and link by looking at clusters generated at some threshold match probability of interest - in this example 99%
#

# %%
clusters = linker_detailed.clustering.cluster_pairwise_predictions_at_threshold(
    predictions, threshold_match_probability=0.99
)
df_clusters = clusters.as_pandas_dataframe().sort_values("cluster_id")
df_clusters.groupby("cluster_id").size().value_counts()

# %% [markdown]
# In this case, we happen to know what the true links are, so we can manually inspect the ones that are doing worst to see what our model is not capturing - i.e. where we have false negatives.
#
# Similarly, we can look at the non-links which are performing the best, to see whether we have an issue with false positives.
#
# Ordinarily we would not have this luxury, and so would need to dig a bit deeper for clues as to how to improve our model, such as manually inspecting records across threshold probabilities,
#

# %%
df_predictions["cluster_l"] = df_predictions["rec_id_l"].apply(
    lambda x: "-".join(x.split("-")[:2])
)
df_predictions["cluster_r"] = df_predictions["rec_id_r"].apply(
    lambda x: "-".join(x.split("-")[:2])
)
df_true_links = df_predictions[
    df_predictions["cluster_l"] == df_predictions["cluster_r"]
].sort_values("match_probability")

# %%
records_to_view = 3
linker_detailed.visualisations.waterfall_chart(
    df_true_links.head(records_to_view).to_dict(orient="records")
)

# %%
df_non_links = df_predictions[
    df_predictions["cluster_l"] != df_predictions["cluster_r"]
].sort_values("match_probability", ascending=False)
linker_detailed.visualisations.waterfall_chart(
    df_non_links.head(records_to_view).to_dict(orient="records")
)

# %% [markdown]
# ## Further refinements
#
# Looking at the non-links we have done well in having no false positives at any substantial match probability --- however looking at some of the true links we can see that there are a few that we are not capturing with sufficient match probability.
#
# We can see that there are a few features that we are not capturing/weighting appropriately
#
# - single-character transpostions, particularly in postcode (which is being lumped in with more 'severe typos'/probable non-matches)
# - given/sur-names being swapped with typos
# - given/sur-names being cross-matches on one only, with no match on the other cross
#
# We will quickly see if we can incorporate these features into a new model. As we are now going into more detail with the inter-relationship between given name and surname, it is probably no longer sensible to model them as independent comparisons, and so we will need to switch to a combined comparison on full name.
#

# %%
# we need to append a full name column to our source data frames
# so that we can use it for term frequency adjustments
dfs[0]["full_name"] = dfs[0]["given_name"] + "_" + dfs[0]["surname"]
dfs[1]["full_name"] = dfs[1]["given_name"] + "_" + dfs[1]["surname"]


extended_model_settings = {
    "unique_id_column_name": "rec_id",
    "link_type": "link_only",
    "blocking_rules_to_generate_predictions": blocking_rules,
    "comparisons": [
        {
            "output_column_name": "Full name",
            "comparison_levels": [
                {
                    "sql_condition": "(given_name_l IS NULL OR given_name_r IS NULL) and (surname_l IS NULL OR surname_r IS NULL)",
                    "label_for_charts": "Null",
                    "is_null_level": True,
                },
                # full name match
                cll.ExactMatchLevel("full_name", term_frequency_adjustments=True),
                # typos - keep levels across full name rather than scoring separately
                cll.JaroWinklerLevel("full_name", 0.9),
                cll.JaroWinklerLevel("full_name", 0.7),
                # name switched
                cll.ColumnsReversedLevel("given_name", "surname"),
                # name switched + typo
                {
                    "sql_condition": "jaro_winkler_similarity(given_name_l, surname_r) + jaro_winkler_similarity(surname_l, given_name_r) >= 1.8",
                    "label_for_charts": "switched + jaro_winkler_similarity >= 1.8",
                },
                {
                    "sql_condition": "jaro_winkler_similarity(given_name_l, surname_r) + jaro_winkler_similarity(surname_l, given_name_r) >= 1.4",
                    "label_for_charts": "switched + jaro_winkler_similarity >= 1.4",
                },
                # single name match
                cll.ExactMatchLevel("given_name", term_frequency_adjustments=True),
                cll.ExactMatchLevel("surname", term_frequency_adjustments=True),
                # single name cross-match
                {
                    "sql_condition": "given_name_l = surname_r OR surname_l = given_name_r",
                    "label_for_charts": "single name cross-matches",
                },  # single name typos
                cll.JaroWinklerLevel("given_name", 0.9),
                cll.JaroWinklerLevel("surname", 0.9),
                # the rest
                cll.ElseLevel(),
            ],
        },
        cl.DateOfBirthComparison(
            "date_of_birth",
            input_is_string=True,
            datetime_format="%Y%m%d",
            invalid_dates_as_null=True,
        ),
        {
            "output_column_name": "Social security ID",
            "comparison_levels": [
                cll.NullLevel("soc_sec_id"),
                cll.ExactMatchLevel("soc_sec_id", term_frequency_adjustments=True),
                cll.DamerauLevenshteinLevel("soc_sec_id", 1),
                cll.DamerauLevenshteinLevel("soc_sec_id", 2),
                cll.ElseLevel(),
            ],
        },
        {
            "output_column_name": "Street number",
            "comparison_levels": [
                cll.NullLevel("street_number"),
                cll.ExactMatchLevel("street_number", term_frequency_adjustments=True),
                cll.DamerauLevenshteinLevel("street_number", 1),
                cll.ElseLevel(),
            ],
        },
        {
            "output_column_name": "Postcode",
            "comparison_levels": [
                cll.NullLevel("postcode"),
                cll.ExactMatchLevel("postcode", term_frequency_adjustments=True),
                cll.DamerauLevenshteinLevel("postcode", 1),
                cll.DamerauLevenshteinLevel("postcode", 2),
                cll.ElseLevel(),
            ],
        },
        # we don't consider further location columns as they will be strongly correlated with postcode
    ],
    "retain_intermediate_calculation_columns": True,
}

# %%
# train
db_api = DuckDBAPI()
dfs_sdf = [db_api.register(df) for df in dfs]
linker_advanced = Linker(dfs_sdf, extended_model_settings)
linker_advanced.training.estimate_probability_two_random_records_match(
    deterministic_rules, recall=0.8
)
# We recommend increasing target rows to 1e8 improve accuracy for u
# values in full name comparison, as we have subdivided the data more finely

# Here, 1e7 for speed
linker_advanced.training.estimate_u_using_random_sampling(max_pairs=1e7)

# %%
session_dob = (
    linker_advanced.training.estimate_parameters_using_expectation_maximisation(
        "l.date_of_birth = r.date_of_birth", estimate_without_term_frequencies=True
    )
)

# %%
session_pc = (
    linker_advanced.training.estimate_parameters_using_expectation_maximisation(
        "l.postcode = r.postcode", estimate_without_term_frequencies=True
    )
)

# %%
linker_advanced.visualisations.parameter_estimate_comparisons_chart()

# %%
linker_advanced.visualisations.match_weights_chart()

# %%
predictions_adv = linker_advanced.inference.predict()
df_predictions_adv = predictions_adv.as_pandas_dataframe()
clusters_adv = linker_advanced.clustering.cluster_pairwise_predictions_at_threshold(
    predictions_adv, threshold_match_probability=0.99
)
df_clusters_adv = clusters_adv.as_pandas_dataframe().sort_values("cluster_id")
df_clusters_adv.groupby("cluster_id").size().value_counts()

# %% [markdown]
# This is a pretty modest improvement on our previous model - however it is worth re-iterating that we should not necessarily expect to recover _all_ matches, as in several cases it may be unreasonable for a model to have reasonable confidence that two records refer to the same entity.
#
# If we wished to improve matters we could iterate on this process - investigating where our model is not performing as we would hope, and seeing how we can adjust these areas to address these shortcomings.
#
