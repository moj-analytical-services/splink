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
# ## Comparing String Similarity and Distance Scores
#
# Splink contains a `comparison_helpers` module which includes some helper functions for comparing the string similarity and distance scores that can help when choosing the most appropriate fuzzy matching function.
#
# For comparing two strings the `comparator_score` function returns the scores for all of the available comparators. E.g. consider a simple inversion "Richard" vs "iRchard":

# %%
from splink.exploratory import similarity_analysis as sa

sa.comparator_score("Richard", "iRchard")

# %% [markdown]
# Now consider a collection of common variations of the name "Richard" - which comparators will consider these variations as sufficiently similar to "Richard"?

# %%
import pandas as pd

data = [
    {"string1": "Richard", "string2": "Richard", "error_type": "None"},
    {"string1": "Richard", "string2": "ichard", "error_type": "Deletion"},
    {"string1": "Richard", "string2": "Richar", "error_type": "Deletion"},
    {"string1": "Richard", "string2": "iRchard", "error_type": "Transposition"},
    {"string1": "Richard", "string2": "Richadr", "error_type": "Transposition"},
    {"string1": "Richard", "string2": "Rich", "error_type": "Shortening"},
    {"string1": "Richard", "string2": "Rick", "error_type": "Nickname/Alias"},
    {"string1": "Richard", "string2": "Ricky", "error_type": "Nickname/Alias"},
    {"string1": "Richard", "string2": "Dick", "error_type": "Nickname/Alias"},
    {"string1": "Richard", "string2": "Rico", "error_type": "Nickname/Alias"},
    {"string1": "Richard", "string2": "Rachael", "error_type": "Different Name"},
    {"string1": "Richard", "string2": "Stephen", "error_type": "Different Name"},
]

df = pd.DataFrame(data)
df

# %% [markdown]
# The `comparator_score_chart` function allows you to compare two lists of strings and how similar the elements are according to the available string similarity and distance metrics.

# %%
sa.comparator_score_chart(data, "string1", "string2")

# %% [markdown]
# Here we can see that all of the metrics are fairly sensitive to transcriptions errors ("Richadr", "Richar", "iRchard"). However, considering nicknames/aliases ("Rick", "Ricky", "Rico"), simple metrics such as Jaccard, Levenshtein and Damerau-Levenshtein tend to be less useful. The same can be said for name shortenings ("Rich"), but to a lesser extent than more complex nicknames. However, even more subtle metrics like Jaro and Jaro-Winkler still struggle to identify less obvious nicknames/aliases such as "Dick". 

# %% [markdown]
# If you would prefer the underlying dataframe instead of the chart, there is the `comparator_score_df` function.

# %%
sa.comparator_score_df(data, "string1", "string2")

# %% [markdown]
# ### Choosing thresholds
#
# We can add distance and similarity thresholds to the comparators to see what strings would be included in a given comparison level:

# %%
sa.comparator_score_threshold_chart(
    data, "string1", "string2", distance_threshold=2, similarity_threshold=0.8
)

# %% [markdown]
# To class our variations on "Richard" in the same `Comparison Level`, a good choice of metric could be Jaro-Winkler with a threshold of 0.8. Lowering the threshold any more could increase the chances for false positives. 
#
# For example, consider a single Jaro-Winkler `Comparison Level` threshold of 0.7 would lead to "Rachael" being considered as providing the same amount evidence for a record matching as "iRchard".
#
# An alternative way around this is to construct a `Comparison` with multiple levels, each corresponding to a different threshold of Jaro-Winkler similarity. For example, below we construct a `Comparison` using the `Comparison Library` function [JaroWinklerAtThresholds](../../api_docs/comparison_library.md#splink.comparison_library.JaroWinklerAtThresholds) with multiple levels for different match thresholds.:

# %%
import splink.comparison_library as cl

first_name_comparison = cl.JaroWinklerAtThresholds("first_name", [0.9, 0.8, 0.7])

# %% [markdown]
# If we print this comparison as a dictionary we can see the underlying SQL.

# %%
first_name_comparison.get_comparison("duckdb").as_dict()

# %% [markdown]
# Where:  
#
# * Exact Match level will catch perfect matches ("Richard").  
# * The 0.9 threshold will catch Shortenings and Typos ("ichard", "Richar", "iRchard", "Richadr",  "Rich").  
# * The 0.8 threshold will catch simple Nicknames/Aliases ("Rick", "Rico").  
# * The 0.7 threshold will catch more complex Nicknames/Aliases ("Ricky"), but will also include less relevant names (e.g. "Rachael"). However, this should not be a concern as the model should give less predictive power (i.e. Match Weight) to this level of evidence.  
# * All other comparisons will end up in the "Else" level  

# %% [markdown]
# ## Phonetic Matching 
#
# There are similar functions available within splink to help users get familiar with phonetic transformations. You can create similar visualisations to string comparators.
#
# To see the phonetic transformations for a single string, there is the `phonetic_transform` function:

# %%
sa.phonetic_transform("Richard")

# %%
sa.phonetic_transform("Steven")

# %% [markdown]
# Now consider a collection of common variations of the name "Stephen". Which phonetic transforms will consider these as sufficiently similar to "Stephen"?

# %%
data = [
    {"string1": "Stephen", "string2": "Stephen", "error_type": "None"},
    {"string1": "Stephen", "string2": "Steven", "error_type": "Spelling Variation"},
    {"string1": "Stephen", "string2": "Stephan", "error_type": "Spelling Variation/Similar Name"},
    {"string1": "Stephen", "string2": "Steve", "error_type": "Nickname/Alias"},
    {"string1": "Stephen", "string2": "Stehpen", "error_type": "Transposition"},
    {"string1": "Stephen", "string2": "tSephen", "error_type": "Transposition"},
    {"string1": "Stephen", "string2": "Stephne", "error_type": "Transposition"},
    {"string1": "Stephen", "string2": "Stphen", "error_type": "Deletion"},
    {"string1": "Stephen", "string2": "Stepheb", "error_type": "Replacement"},
    {"string1": "Stephen", "string2": "Stephanie", "error_type": "Different Name"},
    {"string1": "Stephen", "string2": "Richard", "error_type": "Different Name"},
]


df = pd.DataFrame(data)
df

# %% [markdown]
# The `phonetic_match_chart` function allows you to compare two lists of strings and how similar the elements are according to the available string similarity and distance metrics.

# %%
sa.phonetic_match_chart(data, "string1", "string2")

# %% [markdown]
# Here we can see that all of the algorithms recognise simple phonetically similar names ("Stephen", "Steven"). However, there is some variation when it comes to transposition errors ("Stehpen", "Stephne") with soundex and metaphone-esque giving different results. There is also different behaviour considering different names ("Stephanie").
#
# Given there is no clear winner that captures all of the similar names, it is recommended that phonetic matches are used as a single `Comparison Level` within in a `Comparison` which also includes [string comparators](./comparators.md) in the other levels. To see an example of this, see the [Combining String scores and Phonetic matching](#combining-string-scores-and-phonetic-matching) section of this topic guide.

# %% [markdown]
# If you would prefer the underlying dataframe instead of the chart, there is the `phonetic_transform_df` function.

# %%
sa.phonetic_transform_df(data, "string1", "string2")

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
