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
# # Choosing blocking rules to optimise runtime
#
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/03_Blocking.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# To link records, we need to compare pairs of records and decide which pairs are matches.
#
# For example consider the following two records:
#
# | first_name | surname | dob        | city   | email               |
# | ---------- | ------- | ---------- | ------ | ------------------- |
# | Robert     | Allen   | 1971-05-24 | nan    | roberta25@smith.net |
# | Rob        | Allen   | 1971-06-24 | London | roberta25@smith.net |
#
# These can be represented as a pairwise comparison as follows:
#
# | first_name_l | first_name_r | surname_l | surname_r | dob_l      | dob_r      | city_l | city_r | email_l             | email_r             |
# | ------------ | ------------ | --------- | --------- | ---------- | ---------- | ------ | ------ | ------------------- | ------------------- |
# | Robert       | Rob          | Allen     | Allen     | 1971-05-24 | 1971-06-24 | nan    | London | roberta25@smith.net | roberta25@smith.net |
#
# For most large datasets, it is computationally intractable to compare every row with every other row, since the number of comparisons rises quadratically with the number of records.
#
# Instead we rely on blocking rules, which specify which pairwise comparisons to generate. For example, we could generate the subset of pairwise comparisons where either first name or surname matches.
#
# This is part of a two step process to link data:
#
# 1.  Use blocking rules to generate candidate pairwise record comparisons
#
# 2.  Use a probabilistic linkage model to score these candidate pairs, to determine which ones should be linked
#
# **Blocking rules are the most important determinant of the performance of your linkage job**.
#
# When deciding on your blocking rules, you're trading off accuracy for performance:
#
# - If your rules are too loose, your linkage job may fail.
# - If they're too tight, you may miss some valid links.
#
# This tutorial clarifies what blocking rules are, and how to choose good rules.
#
# ## Blocking rules in Splink
#
# In Splink, blocking rules are specified as SQL expressions.
#
# For example, to generate the subset of record comparisons where the first name and surname matches, we can specify the following blocking rule:
#
# ```python
# from splink import block_on
# block_on("first_name", "surname")
# ```
#
# When executed, this blocking rule will be converted to a SQL statement with the following form:
#
# ```sql
# SELECT ...
# FROM input_tables as l
# INNER JOIN input_tables as r
# ON l.first_name = r.first_name AND l.surname = r.surname
# ```
#
# Since blocking rules are SQL expressions, they can be arbitrarily complex. For example, you could create record comparisons where the initial of the first name and the surname match with the following rule:
#
# ```python
# from splink import block_on
# block_on("substr(first_name, 1, 2)", "surname")
# ```
#

# %% [markdown]
# ## Devising effective blocking rules for prediction
#
# The aims of your blocking rules are twofold:
#
# 1. Eliminate enough non-matching comparison pairs so your record linkage job is small enough to compute
# 2. Eliminate as few truly matching pairs as possible (ideally none)
#
# It is usually impossible to find a single blocking rule which achieves both aims, so we recommend using multiple blocking rules.
#
# When we specify multiple blocking rules, Splink will generate all comparison pairs that meet any one of the rules.
#
# For example, consider the following blocking rule:
#
# `block_on("first_name", "dob")`
#
# This rule is likely to be effective in reducing the number of comparison pairs. It will retain all truly matching pairs, except those with errors or nulls in either the `first_name` or `dob` fields.
#
# Now consider a second blocking rule:
#
# `block_on("email")`.
#
# This will retain all truly matching pairs, except those with errors or nulls in the `email` column.
#
# Individually, these blocking rules are problematic because they exclude true matches where the records contain typos of certain types. But between them, they might do quite a good job.
#
# For a true match to be eliminated by the use of these two blocking rules, it would have to have an error in _both_ `email` AND (`first_name` or `dob`).
#
# This is not completely implausible, but it is significantly less likely than if we'd used a single rule.
#
# More generally, we can often specify multiple blocking rules such that it becomes highly implausible that a true match would not meet at least one of these blocking criteria. This is the recommended approach in Splink. Generally we would recommend between about 3 and 10, though even more is possible.
#
# The question then becomes how to choose what to put in this list.
#

# %% [markdown]
# ## Splink tools to help choose your blocking rules
#
# Splink contains a number of tools to help you choose effective blocking rules. Let's try them out, using our small test dataset:
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
from splink import DuckDBAPI, block_on, splink_datasets

df = splink_datasets.fake_1000

# %% [markdown]
# ### Counting the number of comparisons created by a single blocking rule
#
# On large datasets, some blocking rules imply the creation of trillions of record comparisons, which would cause a linkage job to fail.
#
# Before using a blocking rule in a linkage job, it's therefore a good idea to count the number of records it generates to ensure it is not too loose:
#

# %%
from splink.blocking_analysis import count_comparisons_from_blocking_rule

db_api = DuckDBAPI()
df_sdf = db_api.register(df)

br = block_on("substr(first_name, 1,1)", "surname")

counts = count_comparisons_from_blocking_rule(
    df_sdf,
    blocking_rule=br,
    link_type="dedupe_only",
)

counts

# %%
br = "l.first_name = r.first_name and levenshtein(l.surname, r.surname) < 2"

counts = count_comparisons_from_blocking_rule(
    df_sdf,
    blocking_rule=br,
    link_type="dedupe_only",
)
counts

# %% [markdown]
# The maximum number of comparisons that you can compute will be affected by your choice of SQL backend, and how powerful your computer is.
#
# For linkages in DuckDB on a standard laptop, we suggest using blocking rules that create no more than about 20 million comparisons. For Spark and Athena, try starting with fewer than 100 million comparisons, before scaling up.
#

# %% [markdown]
# ### Finding 'worst offending' values for your blocking rule
#
# Blocking rules can be affected by skew:  some values of a field may be much more common than others, and this can lead to a disproportionate number of comparisons being generated.
#
# It can be useful to identify whether your data is afflicted by this problem. 

# %%
from splink.blocking_analysis import n_largest_blocks

result = n_largest_blocks(
    df_sdf,
    blocking_rule=block_on("city", "first_name"),
    link_type="dedupe_only",
    n_largest=3
)

result.as_pandas_dataframe()

# %% [markdown]
# In this case, we can see that `Oliver`s in `London` will result in 49 comparisons being generated.  This is acceptable on this small dataset, but on a larger dataset, `Oliver`s in `London` could be responsible for many million comparisons.

# %% [markdown]
# ### Counting the number of comparisons created by a list of blocking rules
#
# As noted above, it's usually a good idea to use multiple blocking rules. It's therefore useful to know how many record comparisons will be generated when these rules are applied.
#
# Since the same record comparison may be created by several blocking rules, and Splink automatically deduplicates these comparisons, we cannot simply total the number of comparisons generated by each rule individually.
#
# Splink provides a chart that shows the marginal (additional) comparisons generated by each blocking rule, after deduplication:
#

# %%
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

blocking_rules_for_analysis = [
    block_on("substr(first_name, 1,1)", "surname"),
    block_on("surname"),
    block_on("email"),
    block_on("city", "first_name"),
    "l.first_name = r.first_name and levenshtein(l.surname, r.surname) < 2",
]


cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    df_sdf,
    blocking_rules=blocking_rules_for_analysis,
    link_type="dedupe_only",
)

# %% [markdown]
# ### Digging deeper: Understanding why certain blocking rules create large numbers of comparisons
#
# Finally, we can use the `profile_columns` function we saw in the previous tutorial to understand a specific blocking rule in more depth.
#
# Suppose we're interested in blocking on city and first initial.
#
# Within each distinct value of `(city, first initial)`, all possible pairwise comparisons will be generated.
#
# So for instance, if there are 15 distinct records with `London,J` then these records will result in `n(n-1)/2 = 105` pairwise comparisons being generated.
#
# In a larger dataset, we might observe 10,000 `London,J` records, which would then be responsible for `49,995,000` comparisons.
#
# These high-frequency values therefore have a disproportionate influence on the overall number of pairwise comparisons, and so it can be useful to analyse skew, as follows:
#

# %%
from splink.exploratory import profile_columns

profile_columns(df_sdf, column_expressions=["city || left(first_name,1)"])

# %% [markdown]
# !!! note "Further Reading"
#     :simple-readme: For a deeper dive on blocking, please refer to the [Blocking Topic Guides](../../topic_guides/blocking/blocking_rules.md).
#
#     :material-tools: For more on the blocking tools in Splink, please refer to the [Blocking API documentation](../../api_docs/blocking.md).
#
#     :bar_chart: For more on the charts used in this tutorial, please refer to the [Charts Gallery](../../charts/index.md#blocking).
#

# %% [markdown]
# ## Next steps
#
# Now we have chosen which records to compare, we can use those records to train a linkage model.
#
