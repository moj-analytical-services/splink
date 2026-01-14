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

# %% [raw]
# # Specifying and estimating a linkage model
#
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/04_Estimating_model_parameters.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# In the last tutorial we looked at how we can use blocking rules to generate pairwise record comparisons.
#
# Now it's time to estimate a probabilistic linkage model to score each of these comparisons. The resultant match score is a prediction of whether the two records represent the same entity (e.g. are the same person).
#
# The purpose of estimating the model is to learn the relative importance of different parts of your data for the purpose of data linking.
#
# For example, a match on date of birth is a much stronger indicator that two records refer to the same entity than a match on gender. A mismatch on gender may be a stronger indicate against two records referring than a mismatch on name, since names are more likely to be entered differently.
#
# The relative importance of different information is captured in the (partial) 'match weights', which can be learned from your data. These match weights are then added up to compute the overall match score.
#
# The match weights are are derived from the `m` and `u` parameters of the underlying Fellegi Sunter model. Splink uses various statistical routines to estimate these parameters. Further details of the underlying theory can be found [here](https://www.robinlinacre.com/intro_to_probabilistic_linkage/), which will help you understand this part of the tutorial.
#

# %% [markdown]
# ## Specifying a linkage model
#
# To build a linkage model, the user defines the partial match weights that `splink` needs to estimate. This is done by defining how the information in the input records should be compared.
#
# To be concrete, here is an example comparison:
#
# | first_name_l | first_name_r | surname_l | surname_r | dob_l      | dob_r      | city_l | city_r | email_l             | email_r             |
# | ------------ | ------------ | --------- | --------- | ---------- | ---------- | ------ | ------ | ------------------- | ------------------- |
# | Robert       | Rob          | Allen     | Allen     | 1971-05-24 | 1971-06-24 | nan    | London | roberta25@smith.net | roberta25@smith.net |
#
# What functions should we use to assess the similarity of `Rob` vs. `Robert` in the the `first_name` field?
#
# Should similarity in the `dob` field be computed in the same way, or a different way?
#
# Your job as the developer of a linkage model is to decide what comparisons are most appropriate for the types of data you have.
#
# Splink can then estimate how much weight to place on a fuzzy match of `Rob` vs. `Robert`, relative to an exact match on `Robert`, or a non-match.
#
# Defining these scenarios is done using `Comparison`s.
#

# %% [markdown]
# ### Comparisons
#
# The concept of a `Comparison` has a specific definition within Splink: it defines how data from one or more input columns is compared.
#
# For example, one `Comparison` may represent how similarity is assessed for a person's date of birth.
#
# Another `Comparison` may represent the comparison of a person's name or location.
#
# A model is composed of many `Comparison`s, which between them assess the similarity of all of the columns being used for data linking.
#
# Each `Comparison` contains two or more `ComparisonLevels` which define _n_ discrete gradations of similarity between the input columns within the Comparison.
#
# As such `ComparisonLevels`are nested within `Comparisons` as follows:
#
# ```
# Data Linking Model
# ├─-- Comparison: Date of birth
# │    ├─-- ComparisonLevel: Exact match
# │    ├─-- ComparisonLevel: One character difference
# │    ├─-- ComparisonLevel: All other
# ├─-- Comparison: Surname
# │    ├─-- ComparisonLevel: Exact match on surname
# │    ├─-- ComparisonLevel: All other
# │    etc.
# ```
#
# Our example data would therefore result in the following comparisons, for `dob` and `surname`:
#
# | dob_l      | dob_r      | comparison_level         | interpretation |
# | ---------- | ---------- | ------------------------ | -------------- |
# | 1971-05-24 | 1971-05-24 | Exact match              | great match    |
# | 1971-05-24 | 1971-06-24 | One character difference | fuzzy match    |
# | 1971-05-24 | 2000-01-02 | All other                | bad match      |
#
# <br/>
#
# | surname_l | surname_r | comparison_level | interpretation                                        |
# | --------- | --------- | ---------------- | ----------------------------------------------------- |
# | Rob       | Rob       | Exact match      | great match                                           |
# | Rob       | Jane      | All other        | bad match                                             |
# | Rob       | Robert    | All other        | bad match, this comparison has no notion of nicknames |
#
# More information about specifying comparisons can be found [here](../../topic_guides/comparisons/customising_comparisons.ipynb) and [here](../../topic_guides/comparisons/comparisons_and_comparison_levels.md).
#
# We will now use these concepts to build a data linking model.
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
# Begin by reading in the tutorial data again
from splink import splink_datasets

df = splink_datasets.fake_1000

# %% [markdown]
# ### Specifying the model using comparisons
#
# Splink includes a library of comparison functions at `splink.comparison_library` to make it simple to get started. These are split into two categories:
#
# 1. Generic `Comparison` functions which apply a particular fuzzy matching function. For example, levenshtein distance.
#

# %%
import splink.comparison_library as cl

city_comparison = cl.LevenshteinAtThresholds("city", 2)
print(city_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# 2. `Comparison` functions tailored for specific data types. For example, email.

# %%
email_comparison = cl.EmailComparison("email")
print(email_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# ## Specifying the full settings dictionary
#
# `Comparisons` are specified as part of the Splink `settings`, a Python dictionary which controls all of the configuration of a Splink model:
#

# %%
from splink import Linker, SettingsCreator, block_on, DuckDBAPI, ColumnExpression

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.LevenshteinAtThresholds(ColumnExpression("dob").cast_to_string(), 1),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name", "city"),
        block_on("surname"),

    ],
    retain_intermediate_calculation_columns=True,
)

db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)

# %% [markdown]
# In words, this setting dictionary says:
#
# - We are performing a `dedupe_only` (the other options are `link_only`, or `link_and_dedupe`, which may be used if there are multiple input datasets).
# - When comparing records, we will use information from the `first_name`, `surname`, `dob`, `city` and `email` columns to compute a match score.
# - The `blocking_rules_to_generate_predictions` states that we will only check for duplicates amongst records where either the `first_name AND city` or `surname` is identical.
# - We have enabled [term frequency adjustments](https://moj-analytical-services.github.io/splink/topic_guides/comparisons/term-frequency.html) for the 'city' column, because some values (e.g. `London`) appear much more frequently than others.
# - We have set `retain_intermediate_calculation_columns` and `additional_columns_to_retain` to `True` so that Splink outputs additional information that helps the user understand the calculations. If they were `False`, the computations would run faster.
#

# %% [markdown]
# ## Estimate the parameters of the model
#
# Now that we have specified our linkage model, we need to estimate the [`probability_two_random_records_match`](../../api_docs//settings_dict_guide.md#probability_two_random_records_match), `u`, and `m` parameters.
#
# - The `probability_two_random_records_match` parameter is the probability that two records taken at random from your input data represent a match (typically a very small number).
#
# - The `u` values are the proportion of records falling into each `ComparisonLevel` amongst truly _non-matching_ records.
#
# - The `m` values are the proportion of records falling into each `ComparisonLevel` amongst truly _matching_ records
#
# You can read more about [the theory of what these mean](https://www.robinlinacre.com/m_and_u_values/).
#
# We can estimate these parameters using unlabeled data. If we have labels, then we can estimate them even more accurately.
#
# The rationale for the approach recommended in this tutorial is documented [here](../../topic_guides/training/training_rationale.md).
#

# %% [markdown]
# ### Estimation of `probability_two_random_records_match`
#
# In some cases, the `probability_two_random_records_match` will be known. For example, if you are linking two tables of 10,000 records and expect a one-to-one match, then you should set this value to `1/10_000` in your settings instead of estimating it.
#
# More generally, this parameter is unknown and needs to be estimated.
#
# It can be estimated accurately enough for most purposes by combining a series of deterministic matching rules and a guess of the recall corresponding to those rules. For further details of the rationale behind this appraoch see [here](https://github.com/moj-analytical-services/splink/issues/462#issuecomment-1227027995).
#
# In this example, I guess that the following deterministic matching rules have a recall of about 70%. That means, between them, the rules recover 70% of all true matches.
#

# %%
deterministic_rules = [
    block_on("first_name", "dob"),
    "l.first_name = r.first_name and levenshtein(r.surname, l.surname) <= 2",
    block_on("email")
]

linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)

# %% [markdown]
# ### Estimation of `u` probabilities
#
# Once we have the `probability_two_random_records_match` parameter, we can estimate the `u` probabilities.
#
# We estimate `u` using the `estimate_u_using_random_sampling` method, which doesn't require any labels.
#
# It works by sampling random pairs of records, since most of these pairs are going to be non-matches. Over these non-matches we compute the distribution of `ComparisonLevel`s for each `Comparison`.
#
# For instance, for `gender`, we would find that the the gender matches 50% of the time, and mismatches 50% of the time.
#
# For `dob` on the other hand, we would find that the `dob` matches 1% of the time, has a "one character difference" 3% of the time, and everything else happens 96% of the time.
#
# The larger the random sample, the more accurate the predictions. You control this using the `max_pairs` parameter. For large datasets, we recommend using at least 10 million - but the higher the better and 1 billion is often appropriate for larger datasets.
#

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

# %% [markdown]
# ### Estimation of `m` probabilities
#
# `m` is the trickiest of the parameters to estimate, because we have to have some idea of what the true matches are.
#
# If we have labels, we can directly estimate it.
#
# If we do not have labelled data, the `m` parameters can be estimated using an iterative maximum likelihood approach called Expectation Maximisation.
#
# #### Estimating directly
#
# If we have labels, we can estimate `m` directly using the `estimate_m_from_label_column` method of the linker.
#
# For example, if the entity being matched is persons, and your input dataset(s) contain social security number, this could be used to estimate the m values for the model.
#
# Note that this column does not need to be fully populated. A common case is where a unique identifier such as social security number is only partially populated.
#
# For example (in this tutorial we don't have labels, so we're not actually going to use this):
#
# ```python
# linker.training.estimate_m_from_label_column("social_security_number")
# ```
#
# #### Estimating with Expectation Maximisation
#
# This algorithm estimates the `m` values by generating pairwise record comparisons, and using them to maximise a likelihood function.
#
# Each estimation pass requires the user to configure an estimation blocking rule to reduce the number of record comparisons generated to a manageable level.
#
# In our first estimation pass, we block on `first_name` and `surname`, meaning we will generate all record comparisons that have `first_name` and `surname` exactly equal.
#
# Recall we are trying to estimate the `m` values of the model, i.e. proportion of records falling into each `ComparisonLevel` amongst truly matching records.
#
# This means that, in this training session, we cannot estimate parameter estimates for the `first_name` or `surname` columns, since they will be equal for all the comparisons we do.
#
# We can, however, estimate parameter estimates for all of the other columns. The output messages produced by Splink confirm this.
#

# %%
training_blocking_rule = block_on("first_name", "surname")
training_session_fname_sname = (
    linker.training.estimate_parameters_using_expectation_maximisation(training_blocking_rule)
)

# %% [markdown]
# In a second estimation pass, we block on dob. This allows us to estimate parameters for the `first_name` and `surname` comparisons.
#
# Between the two estimation passes, we now have parameter estimates for all comparisons.
#

# %%
training_blocking_rule = block_on("dob")
training_session_dob = linker.training.estimate_parameters_using_expectation_maximisation(
    training_blocking_rule
)

# %% [markdown]
# Note that Splink includes other algorithms for estimating m and u values, which are documented [here](https://moj-analytical-services.github.io/splink/api_docs/training.html).
#

# %% [markdown]
# ## Visualising model parameters
#
# Splink can generate a number of charts to help you understand your model. For an introduction to these charts and how to interpret them, please see [this](https://www.youtube.com/watch?v=msz3T741KQI&t=507s) video.
#
# The final estimated match weights can be viewed in the match weights chart:
#

# %%
linker.visualisations.match_weights_chart()

# %%
linker.visualisations.m_u_parameters_chart()

# %% [markdown]
# We can also compare the estimates that were produced by the different EM training sessions

# %%
linker.visualisations.parameter_estimate_comparisons_chart()

# %% [markdown]
# ### Saving the model
#
# We can save the model, including our estimated parameters, to a `.json` file, so we can use it in the next tutorial.
#

# %%
settings = linker.misc.save_model_to_json(
    "../demo_settings/saved_model_from_demo.json", overwrite=True
)

# %% [markdown]
# ## Detecting unlinkable records
#
# An interesting application of our trained model that is useful to explore before making any predictions is to detect 'unlinkable' records.
#
# Unlinkable records are those which do not contain enough information to be linked. A simple example would be a record containing only 'John Smith', and null in all other fields. This record may link to other records, but we'll never know because there's not enough information to disambiguate any potential links. Unlinkable records can be found by linking records to themselves - if, even when matched to themselves, they don't meet the match threshold score, we can be sure they will never link to anything.
#

# %%
linker.evaluation.unlinkables_chart()

# %% [markdown]
# In the above chart, we can see that about 1.3% of records in the input dataset are unlinkable at a threshold match weight of 6.11 (correponding to a match probability of around 98.6%)
#

# %% [markdown]
# !!! note "Further Reading"
#
#     :material-tools: For more on the model estimation tools in Splink, please refer to the [Model Training API documentation](../../api_docs/training.md).
#
#     :simple-readme: For a deeper dive on:
#
#     * choosing comparisons, please refer to the [Comparisons Topic Guides](../../topic_guides/comparisons/customising_comparisons.ipynb)
#     * the underlying model theory, please refer to the [Fellegi Sunter Topic Guide](../../topic_guides/theory/fellegi_sunter.md)
#     * model training, please refer to the Model Training Topic Guides (Coming Soon).
#
#     :bar_chart: For more on the charts used in this tutorial, please refer to the [Charts Gallery](../../charts/index.md).
#

# %% [markdown]
# ## Next steps
#
# Now we have trained a model, we can move on to using it predict matching records.
#
