# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Exploratory analysis
#
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/02_Exploratory_analysis.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# Exploratory analysis helps you understand features of your data which are relevant linking or deduplicating your data.
#
# Splink includes a variety of charts to help with this, which are demonstrated in this notebook.
#

# %% [markdown]
# ### Read in the data
#
# For the purpose of this tutorial we will use a 1,000 row synthetic dataset that contains duplicates.
#
# The first five rows of this dataset are printed below.
#
# Note that the cluster column represents the 'ground truth' - a column which tells us with which rows refer to the same person. In most real linkage scenarios, we wouldn't have this column (this is what Splink is trying to estimate.)
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
from splink import  splink_datasets

df = splink_datasets.fake_1000
df = df.drop(columns=["cluster"])
df.head(5)

# %% [markdown]
# ## Analyse missingness
#

# %% [markdown]
# It's important to understand the level of missingness in your data, because columns with higher levels of missingness are less useful for data linking.
#

# %%
from splink.exploratory import completeness_chart
from splink import DuckDBAPI
db_api = DuckDBAPI()
df_sdf = db_api.register(df)
completeness_chart(df_sdf)

# %% [markdown]
# The above summary chart shows that in this dataset, the `email`, `city`, `surname` and `forename` columns contain nulls, but the level of missingness is relatively low (less than 22%).
#

# %% [markdown]
# ## Analyse the distribution of values in your data
#

# %% [markdown]
# The distribution of values in your data is important for two main reasons:
#
# 1. Columns with higher cardinality (number of distinct values) are usually more useful for data linking. For instance, date of birth is a much stronger linkage variable than gender.
#
# 2. The skew of values is important. If you have a `city` column that has 1,000 distinct values, but 75% of them are `London`, this is much less useful for linkage than if the 1,000 values were equally distributed
#
# The `profile_columns()` method creates summary charts to help you understand these aspects of your data.
#
# To profile all columns, leave the column_expressions argument empty.
#

# %%
from splink.exploratory import profile_columns

profile_columns(df_sdf, top_n=10, bottom_n=5)

# %% [markdown]
# This chart is very information-dense, but here are some key takehomes relevant to our linkage:
#
# - There is strong skew in the `city` field with around 20% of the values being `London`. We therefore will probably want to use `term_frequency_adjustments` in our linkage model, so that it can weight a match on London differently to a match on, say, `Norwich`.
#
# - Looking at the "Bottom 5 values by value count", we can see typos in the data in most fields. This tells us this information was possibly entered by hand, or using Optical Character Recognition, giving us an insight into the type of data entry errors we may see.
#
# - Email is a much more uniquely-identifying field than any others, with a maximum value count of 6. It's likely to be a strong linking variable.
#

# %% [markdown]
# !!! note "Further Reading"
#
#     :material-tools: For more on exploratory analysis tools in Splink, please refer to the [Exploratory Analysis API documentation](../../api_docs/exploratory.md).
#
#     :bar_chart: For more on the charts used in this tutorial, please refer to the [Charts Gallery](../../charts/index.md#exploratory-analysis).
#

# %% [markdown]
# ## Next steps
#
# At this point, we have begun to develop a strong understanding of our data. It's time to move on to estimating a linkage model
#
