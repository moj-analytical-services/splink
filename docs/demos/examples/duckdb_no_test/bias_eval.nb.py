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
# !!! note
#
#     This notebook is intended for usage alongside the second [Bias in Data Linking blog.](../../../blog/posts/2024-08-15-bias-continued.md)
#
# This notebook will guide you through a 5-step process for evaluating bias in a data linking pipeline. It offers an isolated approach to **bias detection**, exploring potential mitigation options, before drawing conclusions about the existence of bias in the pipeline.
#
# This approach depends on users having pre-developed hypotheses about bias, based on a thorough understanding of the input data and model design. We’ll illustrate the approach with an example hypothesis, using Splink to show how each step can be practically applied.
#
# Consider this hypothesis:
#
# _I’m working with a 50,000-row dataset in which people appear multiple times over their lives. Because their information changes over time, records relating to the same individuals may not always be identical. I suspect that these changes could negatively impact linkage performance for those who update their details. Since women in this dataset are more likely to change their surname and postcode than men, this negative impact would disproportionately affect women, leading to a gender bias._
#
# ---

# %% [markdown]
# ### **1. Generate synthetic data**
#
# Since the hypothesis deals with changes in surnames and addresses, the synthetic data should include multiple records for the same person with these variations:
#
# The synthetic data should also match the structure and format of the production data used to train the model.

# %%
import pandas as pd

synthetic_base_raw = [
    {"unique_id": 1, "person_id": 1, "first_name": "sarah", "surname": "brown", "dob": "1862-07-11", "birth_place": "london", "postcode_fake": "ba12 0ay", "gender": "female", "occupation": "politician"}
]

synthetic_comparison_raw = [
    {"unique_id": 2, "person_id": 1, "first_name": "sarah", "surname": "brown", "dob": "1862-07-11", "birth_place": "london", "postcode_fake": "ba12 0ay", "gender": "female", "occupation": "politician"},
    {"unique_id": 3, "person_id": 1, "first_name": "sarah", "surname": "doyle", "dob": "1862-07-11", "birth_place": "london", "postcode_fake": "ba12 0ay", "gender": "female", "occupation": "politician"},
    {"unique_id": 4, "person_id": 1, "first_name": "sarah", "surname": "brown", "dob": "1862-07-11", "birth_place": "london", "postcode_fake": "tf3 2ng", "gender": "female", "occupation": "politician"},
    {"unique_id": 5, "person_id": 1, "first_name": "sarah", "surname": "doyle", "dob": "1862-07-11", "birth_place": "london", "postcode_fake": "tf3 2ng", "gender": "female", "occupation": "politician"},
    {"unique_id": 6, "person_id": 2, "first_name": "jane", "surname": "brown", "dob": "1860-01-01", "birth_place": "london", "postcode_fake": "ba12 0ay", "gender": "female", "occupation": "artist"}
]

synthetic_base_df = pd.DataFrame(synthetic_base_raw)
synthetic_comparison_df = pd.DataFrame(synthetic_comparison_raw)

# %%
synthetic_base_df

# %%
synthetic_comparison_df

# %% [markdown]
# The first row is the base individual, with each subsequent row representing a different comparison related to the hypothesis. The first comparison is a self-link to serve as a baseline. Next, rows represent isolated surname changes and postcode changes, and finally a combination of both. 
#
# Since this hypothesis relates to the linkage of records that _look different but are for the same person_, it’s helpful to include a record that _looks similar but is for a different person_ (like a sibling or partner). This will help account for the downstream effects of any mitigation option (Step 4).
#
# ---

# %% [markdown]
# ### **2. Train and investigate model**
#
# Train the model on the real production data (this Splink model is trained in 'model_create_h50k.ipynb' and saved as a json file).  
# You can refer to the [Splink tutorial](https://moj-analytical-services.github.io/splink/demos/tutorials/00_Tutorial_Introduction.html) if you need any further explanation on how to prepare the data and define the model settings. 

# %%
from splink import DuckDBAPI
from splink import Linker, SettingsCreator
from splink import splink_datasets

db_api = DuckDBAPI()

production_df = splink_datasets.historical_50k

# %%
db_api = DuckDBAPI()
production_df_sdf = db_api.register(production_df)
linker = Linker(production_df_sdf, settings='../../demo_settings/model_h50k.json')

# %% [markdown]
# It's useful to visualise the model parameters to learn the relative importance of different parts of your data for linking.
#
# You can do this in Splink by using a match weights chart:

# %%
linker.visualisations.match_weights_chart()


# %% [markdown]
# When developing this model, it's essential to evaluate the parameter weights to ensure they align with the pipeline's goals. However, this approach requires considering the bias hypothesis.
#
# At first glance, the model appears normal, but a few key points stand out regarding the hypothesis (focussed on surname and postcode changes):
#
# - A non-match on the surname is more predictive than a non-match on the first name.
# - A postcode match has the highest predictive weight.
#
# The cumulative effect of these weights makes it difficult to fully understand their impact without generating comparisons.
#
# ---

# %% [markdown]
# ### **3. Perform and evaluate linkage**
#
# In a standard linkage with Splink, all records which meet the criteria of the blocking rules would be compared against each other. However, in bias detection we may not be interested in a lot of the comparisons this process will generate match probabilities for. It can be easier to manually generate the comparisons that are relevant to the hypothesis. 

# %%
def compare_records(base_records, comparison_records, linker):
    results = []
    for record_1 in base_records:
        results.extend(
            linker.inference.compare_two_records(record_1, record_2).as_pandas_dataframe()
            for record_2 in comparison_records
        )
    all_comparisons_df = pd.concat(results, ignore_index=True)
    return all_comparisons_df


# %%
comparisons = compare_records(synthetic_base_raw, synthetic_comparison_raw, linker)


# %% [markdown]
# Look at the resulting match probabilities in terms of the threshold of your pipeline.  
# This pipeline has a high match threshold of 0.999, medium of 0.99, and low of 0.95.

# %%
def highlight_cells(val):
    if val >= 0.999:
        color = '#c4f5bf'  # High threshold, green
    elif val >= 0.99:
        color = '#faf9c0'  # Medium threshold, yellow
    elif val >= 0.95:
        color = '#f5e1bf'  # Low threshold, orange
    else:
        color = '#f5c8bf'  # Below threshold, red
    return f'background-color: {color}'


# %%
columns_of_interest = ['match_weight', 'match_probability', 'unique_id_l', 'unique_id_r', 'first_name_l', 'first_name_r', 'surname_l','surname_r', 'dob_l', 'dob_r','postcode_fake_l', 'postcode_fake_r','birth_place_l', 'birth_place_r', 'occupation_l', 'occupation_r']
comparisons[columns_of_interest].style.map(highlight_cells, subset=['match_probability'])

# %% [markdown]
# The first three comparisons show high match probabilities. A surname change or postcode change alone allow for a linkage at a high match threshold.  
# However, when combined they lower the match probability to 0.9781 - which could only be linked at a low threshold in the example pipeline. 
#
# Additionally, it is useful to note that the sibling scenario has a a higher match probability than the surname and postcode change scenario, and it can be matched at a medium threshold. 
#
# It’s helpful to break these results down further to understand how the individual model parameters are combining to result in these final probabilities. This can be done in Splink using the waterfall chart:

# %%
records_to_plot = comparisons.to_dict(orient="records")
linker.visualisations.waterfall_chart(records_to_plot)

# %% [markdown]
# Taking a closer look at each comparison helps reveal which features have the most impact in terms of the hypothesis. Some key takeaways are:
#
# - Matching on postcode and surname are the strongest indicators of a link. Therefore, when both surname and postcode change, you lose that strong link advantage. It's not that a non-match on surname or postcode is overly detrimental to the records being linked, but without the boost from a surname or postcode match, they only meet the low linkage threshold. 
#
# - In the sibling scenario, you can see the impact of one or two strong predictors in a model can have. Even with three non-matches (first name, DOB, and occupation), matching on both postcode and surname links the records at a medium threshold. 
#
# - As first name matches are less predictive than surname matches the sibling scenario has a more predictive name match than the same person changing their information. 
#
# - Due to the way individual factors are weighted, the combination of non-matches in this scenario means that people who change both their postcode and surname won't be linked at higher thresholds. Given this affects women more than men in our data, it leads to a **gender bias** in the pipeline.
#
# ---

# %% [markdown]
# ### **4. Identify mitigations**
#
# Now that bias has been detected, it's time to explore potential mitigation strategies:
#
# <u>Are there viable technical solutions?</u>
#
# In our example, a change in surname and postcode allows records to be linked at a low threshold. We should consider what can be done in the pipeline to ensure these comparisons are linked.
#
# Any technical solution should align with the hypothesis, as opposed to just pushing comparisons randomly over/under a threshold. Different elements of the pipeline can be altered to address the bias. 
#
# - Altering the _input data_ - there's likely no potential solution here, as the bias stems from legitimate data qualities, not errors. 
# - Updating the _model design_ - a TF adjustment might alter results, but this would be random so wouldn't address the hypothesis. However, **adjusting comparison levels** might help. 
# - Adjusting the _output data_ - since the records link at a low threshold, **lowering the threshold** across the model would result in a link.
#
# <u>Could they negatively impact overall performance?</u>
#
# **Lowering the threshold**
#
# This could link records with surname and postcode changes but might also increase false positives, impacting overall performance. This trade-off needs careful consideration. In synthetic data, for instance, siblings would link at low thresholds. This is just one scenario, and there are most likely others. Given these potential issues, we’re not pursuing a lower threshold.
#
# **Adjusting comparison levels**
#
# We'd need to decide that a factor’s predictive power doesn't fit our hypothesis - but also make sure this applies across the whole model. There are lots of options, like making surname or postcode less predictive, or boosting factors like occupation, first name, DOB, or birthplace. We'll focus on one example to demonstrate the necessary considerations. 
#
# Say we decide to manually make first name as predictive as surname, and we find that this change won’t impact overall performance. Therefore, we can take this technical solution to the next consideration.
#
# <u>Could they introduce further bias?</u>
#
# It's important to think about other groups that this decision could impact. 
#
# In this dataset, there's records relating to a community of Vietnamese people. They share similar postcodes and often list the same birthplace (because the dataset defaults to the country of birth if they were born outside the UK). Many families in this community have the same surname, as do many unrelated individuals (due to less variation in Vietnamese surnames). Additionally, because Vietnamese naming conventions differ from Western ones, surnames are often incorrectly recorded as first names in this dataset. 
#
# Therefore, increasing the predictive power of first name matches will likely lead to more false positives for this community, introducing bias against the Vietnamese population in the dataset. The decision is made to not attempt bias mitigation.
#
# ---

# %% [markdown]
# ### **5. Make a statement about bias** 
#
# When a bias is detected but not mitigated, it’s crucial to explore it further to gain more insight. This helps refine the understanding of the bias and lays the groundwork for impact assessment.
#
# In our example, we can dig deeper into how the link fails with postcode and surname changes. To do this, we’ll create some more synthetic data with partial changes in surname and postcode:

# %%
synthetic_comparison_partial_raw = [
    {"unique_id": 2, "person_id": 1, "first_name": "sarah", "surname": "brown-doyle", "dob": "1862-07-11", "birth_place": "london", "postcode_fake": "tf3 2ng", "gender": "female", "occupation": "politician"},
    {"unique_id": 3, "person_id": 1, "first_name": "sarah", "surname": "doyle", "dob": "1862-07-11", "birth_place": "london", "postcode_fake": "ba13 2ng", "gender": "female", "occupation": "politician"},
    {"unique_id": 4, "person_id": 1, "first_name": "sarah", "surname": "brown-doyle", "dob": "1862-07-11", "birth_place": "london", "postcode_fake": "ba13 2ng", "gender": "female", "occupation": "politician"}
]

synthetic_comparison_partial_df = pd.DataFrame(synthetic_comparison_partial_raw)

# %% [markdown]
# In this scenario, we generate new records to be compared to the base. These represent a partial surname change (double-barrel) and a full postcode change, a partial postcode change (same area, different district) with a full surname change, and a combination of partial postcode and partial surname changes.
#
# We can then use the same model to link these records and examine the resulting match probabilities. 

# %%
comparisons_partial = compare_records(synthetic_base_raw, synthetic_comparison_partial_raw, linker)

comparisons_partial[columns_of_interest].style.map(highlight_cells, subset=['match_probability'])

# %% [markdown]
# In all three cases, the records will remain linked at a high threshold. This insight can help make the final bias statement:
#
# _Given how the model weighs the different parameters to produce match probabilities, a full change in both surname and postcode will lead to a non-link. As women are more likely than men to make these changes in the input data, this will introduce a gender bias in the pipeline._
#
# Now that this bias has been detected, it's important to note that we can't yet determine its exact impact on the linked data. This is because the final results are influenced by many other factors in the pipeline, which were intentionally excluded from this process to isolate the bias. 
#
# Further investigation is needed to understand impact — such as understanding how many records in the input data undergo these changes, or how many records with a full surname and postcode change end up linked or not linked in the final data. 
