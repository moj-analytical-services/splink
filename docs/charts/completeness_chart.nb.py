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
# # `completeness_chart`
#

# %% tags=["hide_input"]
chart

# %% [markdown]
#
# !!! info "At a glance"
#     **Useful for:** Looking at which columns are populated across datasets. 
#
#     **API Documentation:** [completeness_chart()](../api_docs/exploratory.md#splink.exploratory.completeness_chart)
#
#     **What is needed to generate the chart?** A `linker` with some data.

# %% [markdown]
# ### What the chart shows
#
# The `completeness_chart` shows the proportion of populated (non-null) values in the columns of multiple datasets.

# %% [markdown]
# ??? note "What the chart tooltip shows"
#
#     ![](./img/completeness_chart_tooltip.png)
#
#     The tooltip shows a number of values based on the panel that the user is hovering over, including:
#
#     - The dataset and column name
#     - The count and percentage of non-null values in the column for the relelvant dataset.

# %% [markdown]
# <hr>

# %% [markdown]
# ### How to interpret the chart
#
# Each panel represents the percentage of non-null values in a given dataset-column combination. The darker the panel, the lower the percentage of non-null values.
#

# %% [markdown]
# <hr>

# %% [markdown]
# ### Actions to take as a result of the chart
#
# Only choose features that are sufficiently populated across all datasets in a linkage model.
#
#

# %% [markdown]
# ## Worked Example

# %% tags=["hide_output"]
from splink import splink_datasets, DuckDBAPI
from splink.exploratory import completeness_chart

df = splink_datasets.fake_1000

# Split a simple dataset into two, separate datasets which can be linked together.
df_l = df.sample(frac=0.5)
df_r = df.drop(df_l.index)


chart = completeness_chart([df_l, df_r], db_api=DuckDBAPI())
chart
