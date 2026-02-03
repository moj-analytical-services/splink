# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: splink_demos
#     language: python
#     name: splink_demos
# ---

# %% [markdown]
# # Data Prerequisites

# %% [markdown]
#
# Splink requires that you clean your data and assign unique IDs to rows before linking. 
#
# This section outlines the additional data cleaning steps needed before loading data into Splink.
#
#
# ### Unique IDs
#
# - Each input dataset must have a unique ID column, which is unique within the dataset.  By default, Splink assumes this column will be called `unique_id`, but this can be changed with the [`unique_id_column_name`](https://moj-analytical-services.github.io/splink/api_docs/settings_dict_guide.html#unique_id_column_name) key in your Splink settings.  The unique id is essential because it enables Splink to keep track each row correctly. 
#
# ### Conformant input datasets
#
# - Input datasets must be conformant, meaning they share the same column names and data formats. For instance, if one dataset has a "date of birth" column and another has a "dob" column, rename them to match. Ensure data type and number formatting are consistent across both columns. The order of columns in input dataframes is not important.
#
# ### Cleaning
#
# - Ensure data consistency by cleaning your data. This process includes standardizing date formats, matching text case, and handling invalid data. For example, if one dataset uses "yyyy-mm-dd" date format and another uses "mm/dd/yyyy," convert them to the same format before using Splink.  Try also to identify and rectify any obvious data entry errors, such as removing values such as 'Mr' or 'Mrs' from a 'first name' column.
#
# ### Ensure nulls are consistently and correctly represented
#
# - Ensure null values (or other 'not known' indicators) are represented as true nulls, not empty strings. Splink treats null values differently from empty strings, so using true nulls guarantees proper matching across datasets.

# %% [markdown]
# ## Further details on data cleaning and standardisation
#
# Splink performs optimally with cleaned and standardized data. Here is a non-exhaustive list of suggestions for data cleaning rules to enhance matching accuracy:
#
# - Trim leading and trailing whitespace from string values (e.g., " john smith " becomes "john smith").
# - Remove special characters from string values (e.g., "O'Hara" becomes "Ohara").
# - Standardise date formats as strings in "yyyy-mm-dd" format.
# - Replace abbreviations with full words (e.g., standardize "St." and "Street" to "Street").
#
#
