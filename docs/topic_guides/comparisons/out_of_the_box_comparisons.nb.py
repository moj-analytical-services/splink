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
# # Out-of-the-box `Comparisons` for specific data types
#
# Splink has pre-defined `Comparison`s available for variety of data types.
#
# <hr>

# %% [markdown]
# ## DateOfBirthComparison
#
# You can find full API docs for `DateOfBirthComparison` [here](../../api_docs/comparison_library.md#splink.comparison_library.DateOfBirthComparison)

# %%
import splink.comparison_library as cl

date_of_birth_comparison = cl.DateOfBirthComparison(
    "date_of_birth",
    input_is_string=True,
)

# %% [markdown]
# You can view the structure of the comparison as follows:

# %%
print(date_of_birth_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# To see this as a specifications dictionary you can use:

# %% tags=["hide_output"]
date_of_birth_comparison.get_comparison("duckdb").as_dict()

# %% [markdown]
# which can be used as the basis for a more custom comparison, as shown in the [Defining and Customising Comparisons topic guide ](customising_comparisons.ipynb#method-3-providing-the-spec-as-a-dictionary), if desired.
#
# <hr>

# %% [markdown]
# ## Name Comparison
#
# A Name comparison is intended for use on an individual name column (e.g. forename, surname) 
#
# You can find full API docs for `NameComparison` [here](../../api_docs/comparison_library.md#splink.comparison_library.NameComparison)

# %%
import splink.comparison_library as cl

first_name_comparison = cl.NameComparison("first_name")

# %%
print(first_name_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# The [NameComparison](../../api_docs/comparison_library.md)  also allows flexibility to change the parameters and/or fuzzy matching comparison levels.
#
# For example:

# %%
surname_comparison = cl.NameComparison(
    "surname",
    jaro_winkler_thresholds=[0.95, 0.9],
    dmeta_col_name="surname_dmeta",
)
print(surname_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# Where `surname_dm` refers to a column which has used the DoubleMetaphone algorithm on `surname` to give a phonetic spelling. This helps to catch names which sounds the same but have different spellings (e.g. Stephens vs Stevens). For more on Phonetic Transformations, see the [topic guide](./phonetic.md).

# %% [markdown]
# To see this as a specifications dictionary you can call

# %% tags=["hide_output"]
surname_comparison.get_comparison("duckdb").as_dict()

# %% [markdown]
# which can be used as the basis for a more custom comparison, as shown in the [Defining and Customising Comparisons topic guide ](customising_comparisons.ipynb#method-3-providing-the-spec-as-a-dictionary), if desired.
#
# <hr>

# %% [markdown]
# ## Forename and Surname Comparison
#
#
# It can be helpful to construct a single comparison for for comparing the forename and surname because:
#
# 1. The Fellegi-Sunter model **assumes that columns are independent**. We know that forename and surname are usually correlated given the regional variation of names etc, so considering then in a single comparison can help to create better models.
#
#     As a result **term-frequencies** of individual forename and surname individually does not necessarily reflect how common the combination of forename and surname are.  For more information on term-frequencies, see the dedicated [topic guide](term-frequency.md). Combining forename and surname in a single comparison can allows the model to consider the joint term-frequency as well as individual.
#
# 2. It is common for some records to have **swapped forename and surname by mistake**. Addressing forename and surname in a single comparison can allows the model to consider these name inversions.
#
# The `ForenameSurnameComparison` has been designed to accomodate this.
#
# You can find full API docs for `ForenameSurnameComparison` [here](../../api_docs/comparison_library.md#splink.comparison_library.ForenameSurnameComparison)

# %%
import splink.comparison_library as cl

full_name_comparison = cl.ForenameSurnameComparison("forename", "surname")

# %%
print(full_name_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# As noted in the [feature engineering guide](../data_preparation//feature_engineering.md), to take advantage of term frequency adjustments on full name, you need to derive a full name column prior to importing data into Splink.  You then provide the column name using the `forename_surname_concat_col_name` argument:

# %%
full_name_comparison = cl.ForenameSurnameComparison("forename", "surname", forename_surname_concat_col_name="first_and_last_name")

# %% [markdown]
# To see this as a specifications dictionary you can call

# %% tags=["hide_output"]
full_name_comparison.get_comparison("duckdb").as_dict()

# %% [markdown]
# Which can be used as the basis for a more custom comparison, as shown in the [Defining and Customising Comparisons topic guide ](customising_comparisons.ipynb#method-3-providing-the-spec-as-a-dictionary), if desired.

# %% [markdown]
# <hr>

# %% [markdown]
# ## Postcode Comparisons
#
# See [Feature Engineering](../data_preparation/feature_engineering.md#postcodes) for more details.

# %%
import splink.comparison_library as cl

pc_comparison = cl.PostcodeComparison("postcode")

# %%
print(pc_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# If you have derived lat long columns, you can model geographical distances.  

# %%

pc_comparison = cl.PostcodeComparison("postcode", lat_col="lat", long_col="long", km_thresholds=[1,10,50])
print(pc_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# To see this as a specifications dictionary you can call

# %% tags=["hide_output"]
pc_comparison.get_comparison("duckdb").as_dict()

# %% [markdown]
# which can be used as the basis for a more custom comparison, as shown in the [Defining and Customising Comparisons topic guide ](customising_comparisons.ipynb#method-3-providing-the-spec-as-a-dictionary), if desired.

# %% [markdown]
# <hr>

# %% [markdown]
# ## Email Comparison
#
# You can find full API docs for `EmailComparison` [here](../../api_docs/comparison_library.md#splink.comparison_library.EmailComparison)

# %%
import splink.comparison_library as cl

email_comparison = cl.EmailComparison("email")

# %%
print(email_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# To see this as a specifications dictionary you can call

# %% tags=["hide_output"]
email_comparison.as_dict()

# %% [markdown]
# which can be used as the basis for a more custom comparison, as shown in the [Defining and Customising Comparisons topic guide ](customising_comparisons.ipynb#method-3-providing-the-spec-as-a-dictionary), if desired.
