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
# # Choosing String Comparators
#
# When building a Splink model, one of the most important aspects is defining the [`Comparisons`](../comparisons/comparisons_and_comparison_levels.md) and [`Comparison Levels`](../comparisons/comparisons_and_comparison_levels.md) that the model will train on. Each `Comparison Level` within a `Comparison` should contain a different amount of evidence that two records are a match, to which the model can assign a match weight. When considering different amounts of evidence for the model, it is helpful to explore fuzzy matching as a way of distinguishing strings that are similar, but not the same, as one another.
#
# This guide is intended to show how Splink's string comparators perform in different situations in order to help choosing the most appropriate comparator for a given column as well as the most appropriate threshold (or thresholds).
# For descriptions and examples of each string comparators available in Splink, see the dedicated [topic guide](./comparators.md).

# %% [markdown]
# ## What options are available when comparing strings?
#
# There are three main classes of string comparator that are considered within Splink:
#
# 1. **String Similarity Scores**  
# 2. **String Distance Scores**  
# 3. **Phonetic Matching**  
#
# where  
#
# **String Similarity Scores** are scores between 0 and 1 indicating how similar two strings are. 0 represents two completely dissimilar strings and 1 represents identical strings. E.g. [Jaro-Winkler Similarity](comparators.md#jaro-winkler-similarity).  
#
# **String Distance Scores** are integer distances, counting the number of operations to convert one string into another. A lower string distance indicates more similar strings. E.g. [Levenshtein Distance](comparators.md#levenshtein-distance).  
#
# **Phonetic Matching** is whether two strings are phonetically similar. The two strings are passed through a [phonetic transformation algorithm](phonetic.md) and then the resulting phonetic codes are matched. E.g. [Double Metaphone](phonetic.md#double-metaphone).

# %% [markdown]
# ## Choosing thresholds
#
# Choose thresholds for a comparator based on the false-positive and false-negative
# trade-off for the data being linked. Splink's comparison library supports multiple
# thresholds in one comparison level.

# %% [markdown]
# Multiple Jaro-Winkler thresholds let the model distinguish stronger from weaker
# evidence. The following example uses [JaroWinklerAtThresholds](../../api_docs/comparison_library.md#splink.comparison_library.JaroWinklerAtThresholds):

# %%
import splink.comparison_library as cl

first_name_comparison = cl.JaroWinklerAtThresholds("first_name", [0.9, 0.8, 0.7])

# %% [markdown]
# If we print this comparison as a dictionary we can see the underlying SQL.

# %%
first_name_comparison.get_comparison("duckdb").as_dict()

# %% [markdown]
# Add an exact-match level and an else level around the thresholds to handle
# identical values and values below the lowest threshold.

# %% [markdown]
# ## Phonetic matching
#
# Phonetic transformations can provide one comparison level alongside string
# comparators. See the [phonetic transformations guide](phonetic.md) for the
# available algorithms and how to derive the required columns.

# %% [markdown]
# ## Combining String scores and Phonetic matching
#
# Once you have considered all of the string comparators and phonetic transforms for a given column, you may decide that you would like to have multiple comparison levels including a combination of options.
#
# For this you can construct a custom comparison to catch all of the edge cases you want. For example, if you decide that the comparison for `first_name` in the model should consider:
#
# 1. A `Dmetaphone` level for phonetic similarity
# 2. A `Levenshtein` level with distance of 2 for typos
# 3. A `Jaro-Winkler` level with similarity 0.8 for fuzzy matching
#

# %%
import splink.comparison_library as cl
import splink.comparison_level_library as cll
first_name_comparison = cl.CustomComparison(
    output_column_name="first_name",
    comparison_levels=[
        cll.NullLevel("first_name"),
        cll.ExactMatchLevel("first_name"),
        cll.JaroWinklerLevel("first_name", 0.9),
        cll.LevenshteinLevel("first_name", 0.8),
        cll.ArrayIntersectLevel("first_name_dm", 1),
        cll.ElseLevel()
    ]
)

print(first_name_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# where `first_name_dm` refers to a column in the dataset which has been created during the [feature engineering](../data_preparation/feature_engineering.md#phonetic-transformations) step to give the `Dmetaphone` transform of `first_name`.
