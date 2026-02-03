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
# # Extracting partial strings
#
# It can sometimes be useful to make comparisons based on substrings or parts of column values. For example, one approach to comparing postcodes is to consider their constituent components, e.g. area, district, etc (see [Featuring Engineering](../data_preparation/feature_engineering.md) for more details).
#
# We can use functions such as substrings and regular expressions to enable users to compare strings without needing to engineer new features from source data.
#
# Splink supports this functionality via the use of the `ComparisonExpression`.
#
# ## Examples
#
# ### 1. Exact match on postcode area
#
# Suppose you wish to make comparisons on a postcode column in your data, however only care about finding links between people who share the same area code (given by the first 1 to 2 letters of the postcode). The regular expression to pick out the first two characters is `^[A-Z]{1,2}`:
#

# %%
import splink.comparison_level_library as cll
from splink import ColumnExpression

pc_ce = ColumnExpression("postcode").regex_extract("^[A-Z]{1,2}")
print(cll.ExactMatchLevel(pc_ce).get_comparison_level("duckdb").sql_condition)

# %% [markdown]
# We may therefore configure a comparison as follows:

# %%
from splink.comparison_library import CustomComparison

cc = CustomComparison(
    output_column_name="postcode",
    comparison_levels=[
        cll.NullLevel("postcode"),
        cll.ExactMatchLevel(pc_ce),
        cll.ElseLevel()
    ]

)
print(cc.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# | person_id_l | person_id_r | postcode_l | postcode_r | comparison_level |
# |-------------|-------------|------------|------------|------------------|
# | 7           | 1           | **SE**1P 0NY   | **SE**1P 0NY   | exact match      |
# | 5           | 1           | **SE**2 4UZ    | **SE**1P 0NY   | exact match      |
# | 9           | 2           | **SW**14 7PQ   | **SW**3 9JG    | exact match      |
# | 4           | 8           | **N**7 8RL     | **EC**2R 8AH   | else level       |
# | 6           | 3           |            | **SE**2 4UZ    | null level       |

# %% [markdown]
# ### 2. Exact match on initial
#
# In this example we use the `.substr` function to create a comparison level based on the first letter of a column value. 
#
# Note that the `substr` function is 1-indexed, so the first character is given by `substr(1, 1)`:  The first two characters would be given by `substr(1, 2)`.

# %%
import splink.comparison_level_library as cll
from splink import ColumnExpression

initial = ColumnExpression("first_name").substr(1,1)
print(cll.ExactMatchLevel(initial).get_comparison_level("duckdb").sql_condition)

# %% [markdown]
# ## Additional info
#
# Regular expressions containing “\” (the python escape character) are tricky to make work with the Spark linker due to escaping so consider using alternative syntax, for example replacing “\d” with “[0-9]”.
#
# Different regex patterns can achieve the same result but with more or less efficiency. You might want to consider optimising your regular expressions to improve performance (see [here](https://www.loggly.com/blog/regexes-the-bad-better-best/), for example).
