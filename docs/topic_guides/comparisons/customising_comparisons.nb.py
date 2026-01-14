# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: 'Python 3.9.2 (''splink-venv'': venv)'
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Defining and customising how record comparisons are made
#
# A key feature of Splink is the ability to customise how record comparisons are made - that is, how similarity is defined for different data types.  For example, the definition of similarity that is appropriate for a date of birth field is different than for a first name field.
#
# By tailoring the definitions of similarity, linking models are more effectively able to distinguish between different gradations of similarity, leading to more accurate data linking models.
#
# ## `Comparisons` and `ComparisonLevels`
#
# [Recall that](./comparisons_and_comparison_levels.md) a Splink model contains a collection of `Comparisons` and `ComparisonLevels` organised in a hierarchy.  
#
# Each `ComparisonLevel` defines the different gradations of similarity that make up a `Comparison`.
#
# An example is as follows:
#
# ```
# Data Linking Model
# ├─-- Comparison: Date of birth
# │    ├─-- ComparisonLevel: Exact match
# │    ├─-- ComparisonLevel: Up to one character difference
# │    ├─-- ComparisonLevel: Up to three character difference
# │    ├─-- ComparisonLevel: All other
# ├─-- Comparison: Name
# │    ├─-- ComparisonLevel: Exact match on first name and surname
# │    ├─-- ComparisonLevel: Exact match on first name
# │    ├─-- etc.
# ```

# %% [markdown]
# ### Three ways of specifying Comparisons
#
# In Splink, there are three ways of specifying `Comparisons`:
#
# - Using 'out-of-the-box' `Comparison`s (Most simple/succinct)
# - Composing pre-defined `ComparisonLevels` 
# - Writing a full dictionary spec of a `Comparison` by hand (most verbose/flexible)

# %% [markdown]
# <hr>

# %% [markdown]
# ## Method 1: Using the `ComparisonLibrary` 
#
# The `ComparisonLibrary`  contains pre-baked similarity functions that cover many common use cases.
#
# These functions generate an entire `Comparison`, composed of several `ComparisonLevels`.
#
# You can find a listing of all available `Comparison`s at the page for its API documentation [here](../../api_docs/comparison_library.md)
#
#
# The following provides an example of using the `ExactMatch` `Comparison`, and producing the description (with associated SQL) for the `duckdb` backend:

# %%
import splink.comparison_library as cl

first_name_comparison = cl.ExactMatch("first_name")
print(first_name_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# Note that, under the hood, these functions generate a Python dictionary, which conforms to the underlying `.json` specification of a model:

# %%
first_name_comparison.get_comparison("duckdb").as_dict()

# %% [markdown]
# We can now generate a second, more complex comparison using one of our data-specific comparisons, the `PostcodeComparison`:

# %%
pc_comparison = cl.PostcodeComparison("postcode")
print(pc_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# For a deep dive on out of the box comparisons, see the dedicated [topic guide](./out_of_the_box_comparisons.ipynb).
#
# Comparisons can be further configured using the `.configure()` method - full API docs [here](../../api_docs/comparison_library.md#splink.internals.comparison_creator.ComparisonCreator.configure).
#
# <hr>

# %% [markdown]
# ## Method 2: `ComparisonLevels`
#
# `ComparisonLevels` provide a lower-level API that allows you to compose your own comparisons.
#
# For example, the user may wish to specify a comparison that has levels for a match on soundex and jaro_winkler of the `first_name` field.  
#
# The below example assumes the user has derived a column `soundex_first_name` which contains the soundex of the first name.

# %%
from splink.comparison_library import CustomComparison
import splink.comparison_level_library as cll

custom_name_comparison = CustomComparison(
    output_column_name="first_name",
    comparison_levels=[
        cll.NullLevel("first_name"),
        cll.ExactMatchLevel("first_name").configure(tf_adjustment_column="first_name"),
        cll.ExactMatchLevel("soundex_first_name").configure(
            tf_adjustment_column="soundex_first_name"
        ),
        cll.ElseLevel(),
    ],
)

print(custom_name_comparison.get_comparison("duckdb").human_readable_description)

# %% [markdown]
# This can now be specified in the settings dictionary as follows:

# %%
from splink import SettingsCreator, block_on

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    comparisons=[
        custom_name_comparison,
        cl.LevenshteinAtThresholds("dob", [1, 2]),
    ],
)

# %% [markdown]
# To inspect the custom comparison as a dictionary, you can call `custom_name_comparison.get_comparison("duckdb").as_dict()`

# %% [markdown]
# Note that `ComparisonLevels` can be further configured using the `.configure()` method - full API documentation [here](../../api_docs/comparison_level_library.md#splink.internals.comparison_creator.ComparisonLevelCreator.configure)  
#
# <hr>

# %% [markdown]
# ## Method 3: Providing the spec as a dictionary
#
# Behind the scenes in Splink, all `Comparisons` are eventually turned into a dictionary which conforms to [the formal `jsonschema` specification of the settings dictionary](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json) and [here](https://moj-analytical-services.github.io/splink/).
#
# The library functions described above are convenience functions that provide a shorthand way to produce valid dictionaries.
#
# For maximum control over your settings, you can specify your comparisons as a dictionary.

# %%
comparison_first_name = {
    "output_column_name": "first_name",
    "comparison_levels": [
        {
            "sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
            "label_for_charts": "Null",
            "is_null_level": True,
        },
        {
            "sql_condition": "first_name_l = first_name_r",
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "first_name",
            "tf_adjustment_weight": 1.0,
            "tf_minimum_u_value": 0.001,
        },
        {
            "sql_condition": "dmeta_first_name_l = dmeta_first_name_r",
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "dmeta_first_name",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "jaro_winkler_sim(first_name_l, first_name_r) > 0.8",
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "first_name",
            "tf_adjustment_weight": 0.5,
            "tf_minimum_u_value": 0.001,
        },
        {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
    ],
}

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ],
    comparisons=[
        comparison_first_name,
        cl.LevenshteinAtThresholds("dob", [1, 2]),
    ],
)


# %% [markdown]
# ## Examples
#
# Below are some examples of how you can define the same comparison, but through different methods.

# %% [markdown]
#
# ### Exact match Comparison with Term-Frequency Adjustments
#
#
#
# ===+ "Comparison Library"
#
#     ```py
#     import splink.comparison_library as cl
#
#     first_name_comparison = cl.ExactMatch("first_name").configure(
#         term_frequency_adjustments=True
#     )
#     ```
#
# === "Comparison Level Library"
#
#     ```py
#     import splink.comparison_level_library as cll
#
#     first_name_comparison = cl.CustomComparison(
#         output_column_name="first_name",
#         comparison_description="Exact match vs. anything else",
#         comparison_levels=[
#             cll.NullLevel("first_name"),
#             cll.ExactMatchLevel("first_name").configure(tf_adjustment_column="first_name"),
#             cll.ElseLevel(),
#         ],
#     )
#     ```
#     
# === "Settings Dictionary"
#
#     ```py
#     first_name_comparison = {
#         'output_column_name': 'first_name',
#         'comparison_levels': [
#             {
#                 'sql_condition': '"first_name_l" IS NULL OR "first_name_r" IS NULL',
#                 'label_for_charts': 'Null',
#                 'is_null_level': True
#             },
#             {
#                 'sql_condition': '"first_name_l" = "first_name_r"',
#                 'label_for_charts': 'Exact match',
#                 'tf_adjustment_column': 'first_name',
#                 'tf_adjustment_weight': 1.0
#             },
#             {
#                 'sql_condition': 'ELSE', 
#                 'label_for_charts': 'All other comparisons'
#             }],
#         'comparison_description': 'Exact match vs. anything else'
#     }
#
#     ```
# Each of which gives
#
# ```json
# {
#     'output_column_name': 'first_name',
#     'comparison_levels': [
#         {
#             'sql_condition': '"first_name_l" IS NULL OR "first_name_r" IS NULL',
#             'label_for_charts': 'Null',
#             'is_null_level': True
#         },
#         {
#             'sql_condition': '"first_name_l" = "first_name_r"',
#             'label_for_charts': 'Exact match',
#             'tf_adjustment_column': 'first_name',
#             'tf_adjustment_weight': 1.0
#         },
#         {
#             'sql_condition': 'ELSE', 
#             'label_for_charts': 'All other comparisons'
#         }],
#     'comparison_description': 'Exact match vs. anything else'
# }
# ```
# in your settings dictionary.

# %% [markdown]
# ### Levenshtein Comparison
#
#
#
# ===+ "Comparison Library"
#
#     ```py
#     import splink.comparison_library as cl
#
#     email_comparison = cl.LevenshteinAtThresholds("email", [2, 4])
#     ```
#
# === "Comparison Level Library"
#
#     ```py
#     import splink.comparison_library as cl
#     import splink.comparison_level_library as cll
#
#     email_comparison = cl.CustomComparison(
#         output_column_name="email",
#         comparison_description="Exact match vs. Email within levenshtein thresholds 2, 4 vs. anything else",
#         comparison_levels=[
#             cll.NullLevel("email"),
#             cll.LevenshteinLevel("email", distance_threshold=2),
#             cll.LevenshteinLevel("email", distance_threshold=4),
#             cll.ElseLevel(),
#         ],
#     )
#     ```
#
# === "Settings Dictionary"
#
#     ```py
#     email_comparison = {
#         'output_column_name': 'email',
#         'comparison_levels': [{'sql_condition': '"email_l" IS NULL OR "email_r" IS NULL',
#         'label_for_charts': 'Null',
#         'is_null_level': True},
#         {
#             'sql_condition': '"email_l" = "email_r"',
#             'label_for_charts': 'Exact match'
#         },
#         {
#             'sql_condition': 'levenshtein("email_l", "email_r") <= 2',
#             'label_for_charts': 'Levenshtein <= 2'
#         },
#         {
#             'sql_condition': 'levenshtein("email_l", "email_r") <= 4',
#             'label_for_charts': 'Levenshtein <= 4'
#         },
#         {
#             'sql_condition': 'ELSE', 
#             'label_for_charts': 'All other comparisons'
#         }],
#         'comparison_description': 'Exact match vs. Email within levenshtein thresholds 2, 4 vs. anything else'}
#     ```
#
# Each of which gives
#
# ```json
# {
#     'output_column_name': 'email',
#     'comparison_levels': [
#         {
#             'sql_condition': '"email_l" IS NULL OR "email_r" IS NULL',
#             'label_for_charts': 'Null',
#             'is_null_level': True},
#         {
#             'sql_condition': '"email_l" = "email_r"',
#             'label_for_charts': 'Exact match'
#         },
#         {
#             'sql_condition': 'levenshtein("email_l", "email_r") <= 2',
#             'label_for_charts': 'Levenshtein <= 2'
#         },
#         {
#             'sql_condition': 'levenshtein("email_l", "email_r") <= 4',
#             'label_for_charts': 'Levenshtein <= 4'
#         },
#         {
#             'sql_condition': 'ELSE', 
#             'label_for_charts': 'All other comparisons'
#         }],
#     'comparison_description': 'Exact match vs. Email within levenshtein thresholds 2, 4 vs. anything else'
# }
# ```
#
# in your settings dictionary.
