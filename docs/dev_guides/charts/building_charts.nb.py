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
# # Building a new chart in Splink
#
# As mentioned in the [Understanding Splink Charts topic guide](./understanding_and_editing_charts.md), splink charts are made up of three distinct parts:
#
# 1. A function to create the dataset for the chart 
# 2. A template chart definition (in a json file)
# 3. A function to read the chart definition, add the data to it, and return the chart itself 
#
# ## Worked Example
#
# Below is a worked example of how to create a new chart that shows all comparisons levels ordered by match weight:

# %% tags=["hide_output"]
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets
df = splink_datasets.fake_1000

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
      cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.DateOfBirthComparison("dob", input_is_string=True),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.LevenshteinAtThresholds("email", 2),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name", "dob"),
        block_on("surname"),
    ]
)

linker = Linker(df, settings,DuckDBAPI())
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
for rule in [block_on("first_name"), block_on("dob")]:
    linker.training.estimate_parameters_using_expectation_maximisation(rule)

# %% [markdown]
# ## Generate data for chart

# %%
# Take linker object and extract complete settings dict
records = linker._settings_obj._parameters_as_detailed_records

cols_to_keep = [
    "comparison_name",
    "sql_condition",
    "label_for_charts",
    "m_probability",
    "u_probability",
    "bayes_factor",
    "log2_bayes_factor",
    "comparison_vector_value"
]

# Keep useful information for a match weights chart
records = [{k: r[k] for k in cols_to_keep}
           for r in records
           if r["comparison_vector_value"] != -1 and r["comparison_sort_order"] != -1]

records[:3]

# %% [markdown]
#

# %% [markdown]
# ## Create a chart template
#
# ### Build prototype chart in Altair

# %%
import pandas as pd
import altair as alt

df = pd.DataFrame(records)

# Need a unique name for each comparison level - easier to create in pandas than altair
df["cl_id"] = df["comparison_name"] + "_" + \
    df["comparison_vector_value"].astype("str")

# Simple start - bar chart with x, y and color encodings
alt.Chart(df).mark_bar().encode(
    y="cl_id",
    x="log2_bayes_factor",
    color="comparison_name"
)

# %% [markdown]
# #### Sort bars, edit axes/titles
#

# %%
alt.Chart(df).mark_bar().encode(
    y=alt.Y("cl_id",
        sort="-x",
        title="Comparison level"
    ),
    x=alt.X("log2_bayes_factor",
        title="Comparison level match weight = log2(m/u)",
        scale=alt.Scale(domain=[-10,10])
    ),
    color="comparison_name"
).properties(
    title="New Chart - WOO!"
).configure_view(
    step=15
)


# %% [markdown]
# #### Add tooltip

# %%
alt.Chart(df).mark_bar().encode(
    y=alt.Y("cl_id",
            sort="-x",
            title="Comparison level"
            ),
    x=alt.X("log2_bayes_factor",
            title="Comparison level match weight = log2(m/u)",
            scale=alt.Scale(domain=[-10, 10])
            ),
    color="comparison_name",
    tooltip=[
        "comparison_name",
        "label_for_charts",
        "sql_condition",
        "m_probability",
        "u_probability",
        "bayes_factor",
        "log2_bayes_factor"
        ]
).properties(
    title="New Chart - WOO!"
).configure_view(
    step=15
)


# %% [markdown]
# #### Add text layer
#

# %%
# Create base chart with shared data and encodings (mark type not specified)
base = alt.Chart(df).encode(
    y=alt.Y("cl_id",
            sort="-x",
            title="Comparison level"
            ),
    x=alt.X("log2_bayes_factor",
            title="Comparison level match weight = log2(m/u)",
            scale=alt.Scale(domain=[-10, 10])
            ),
    tooltip=[
        "comparison_name",
        "label_for_charts",
        "sql_condition",
        "m_probability",
        "u_probability",
        "bayes_factor",
        "log2_bayes_factor"
    ]
)

# Build bar chart from base (color legend made redundant by text labels)
bar = base.mark_bar().encode(
    color=alt.Color("comparison_name", legend=None)
)

# Build text layer from base
text = base.mark_text(dx=0, align="right").encode(
    text="comparison_name"
)

# Final layered chart
chart = bar + text

# Add global config
chart.resolve_axis(
    y="shared",
    x="shared"
).properties(
    title="New Chart - WOO!"
).configure_view(
    step=15
)


# %% [markdown]
# Sometimes things go wrong in Altair and it's not clear why or how to fix it. If the docs and Stack Overflow don't have a solution, the answer is usually that Altair is making decisions under the hood about the Vega-Lite schema that are out of your control.
#
# In this example, the sorting of the y-axis is broken when layering charts. If we show `bar` and `text` side-by-side, you can see they work as expected, but the sorting is broken in the layering process.

# %%
bar | text

# %% [markdown]
# Once we get to this stage (or whenever you're comfortable), we can switch to Vega-Lite by exporting the JSON from our `chart` object, or opening the chart in the Vega-Lite editor.
#
# ```py
# chart.to_json()
# ```

# %% [markdown]
# ??? note "Chart JSON"
#       ```json
#         {
#         "$schema": "https://vega.github.io/schema/vega-lite/v5.8.0.json",
#         "config": {
#           "view": {
#             "continuousHeight": 300,
#             "continuousWidth": 300
#           }
#         },
#         "data": {
#           "name": "data-3901c03d78701611834aa82ab7374cce"
#         },
#         "datasets": {
#           "data-3901c03d78701611834aa82ab7374cce": [
#             {
#               "bayes_factor": 86.62949969575988,
#               "cl_id": "first_name_4",
#               "comparison_name": "first_name",
#               "comparison_vector_value": 4,
#               "label_for_charts": "Exact match first_name",
#               "log2_bayes_factor": 6.436786480320881,
#               "m_probability": 0.5018941916173814,
#               "sql_condition": "\"first_name_l\" = \"first_name_r\"",
#               "u_probability": 0.0057935713975033705
#             },
#             {
#               "bayes_factor": 82.81743551783742,
#               "cl_id": "first_name_3",
#               "comparison_name": "first_name",
#               "comparison_vector_value": 3,
#               "label_for_charts": "Damerau_levenshtein <= 1",
#               "log2_bayes_factor": 6.371862624533329,
#               "m_probability": 0.19595791797531015,
#               "sql_condition": "damerau_levenshtein(\"first_name_l\", \"first_name_r\") <= 1",
#               "u_probability": 0.00236614327345483
#             },
#             {
#               "bayes_factor": 35.47812468678278,
#               "cl_id": "first_name_2",
#               "comparison_name": "first_name",
#               "comparison_vector_value": 2,
#               "label_for_charts": "Jaro_winkler_similarity >= 0.9",
#               "log2_bayes_factor": 5.148857848140163,
#               "m_probability": 0.045985303626033085,
#               "sql_condition": "jaro_winkler_similarity(\"first_name_l\", \"first_name_r\") >= 0.9",
#               "u_probability": 0.001296159366708712
#             },
#             {
#               "bayes_factor": 11.266641370022352,
#               "cl_id": "first_name_1",
#               "comparison_name": "first_name",
#               "comparison_vector_value": 1,
#               "label_for_charts": "Jaro_winkler_similarity >= 0.8",
#               "log2_bayes_factor": 3.493985601438375,
#               "m_probability": 0.06396730257493154,
#               "sql_condition": "jaro_winkler_similarity(\"first_name_l\", \"first_name_r\") >= 0.8",
#               "u_probability": 0.005677583982137938
#             },
#             {
#               "bayes_factor": 0.19514855669673956,
#               "cl_id": "first_name_0",
#               "comparison_name": "first_name",
#               "comparison_vector_value": 0,
#               "label_for_charts": "All other comparisons",
#               "log2_bayes_factor": -2.357355302129234,
#               "m_probability": 0.19219528420634394,
#               "sql_condition": "ELSE",
#               "u_probability": 0.9848665419801952
#             },
#             {
#               "bayes_factor": 113.02818119005431,
#               "cl_id": "surname_4",
#               "comparison_name": "surname",
#               "comparison_vector_value": 4,
#               "label_for_charts": "Exact match surname",
#               "log2_bayes_factor": 6.820538712806792,
#               "m_probability": 0.5527050424941531,
#               "sql_condition": "\"surname_l\" = \"surname_r\"",
#               "u_probability": 0.004889975550122249
#             },
#             {
#               "bayes_factor": 80.61351958508214,
#               "cl_id": "surname_3",
#               "comparison_name": "surname",
#               "comparison_vector_value": 3,
#               "label_for_charts": "Damerau_levenshtein <= 1",
#               "log2_bayes_factor": 6.332949906378981,
#               "m_probability": 0.22212752320956386,
#               "sql_condition": "damerau_levenshtein(\"surname_l\", \"surname_r\") <= 1",
#               "u_probability": 0.0027554624131641246
#             },
#             {
#               "bayes_factor": 48.57568460485815,
#               "cl_id": "surname_2",
#               "comparison_name": "surname",
#               "comparison_vector_value": 2,
#               "label_for_charts": "Jaro_winkler_similarity >= 0.9",
#               "log2_bayes_factor": 5.602162423566203,
#               "m_probability": 0.0490149338194711,
#               "sql_condition": "jaro_winkler_similarity(\"surname_l\", \"surname_r\") >= 0.9",
#               "u_probability": 0.0010090425738347498
#             },
#             {
#               "bayes_factor": 13.478820689774516,
#               "cl_id": "surname_1",
#               "comparison_name": "surname",
#               "comparison_vector_value": 1,
#               "label_for_charts": "Jaro_winkler_similarity >= 0.8",
#               "log2_bayes_factor": 3.752622370380284,
#               "m_probability": 0.05001678986356945,
#               "sql_condition": "jaro_winkler_similarity(\"surname_l\", \"surname_r\") >= 0.8",
#               "u_probability": 0.003710768991942586
#             },
#             {
#               "bayes_factor": 0.1277149376863226,
#               "cl_id": "surname_0",
#               "comparison_name": "surname",
#               "comparison_vector_value": 0,
#               "label_for_charts": "All other comparisons",
#               "log2_bayes_factor": -2.969000820703079,
#               "m_probability": 0.1261357106132424,
#               "sql_condition": "ELSE",
#               "u_probability": 0.9876347504709363
#             },
#             {
#               "bayes_factor": 236.78351486807742,
#               "cl_id": "dob_5",
#               "comparison_name": "dob",
#               "comparison_vector_value": 5,
#               "label_for_charts": "Exact match",
#               "log2_bayes_factor": 7.887424832202931,
#               "m_probability": 0.41383785481447766,
#               "sql_condition": "\"dob_l\" = \"dob_r\"",
#               "u_probability": 0.0017477477477477479
#             },
#             {
#               "bayes_factor": 65.74625268345359,
#               "cl_id": "dob_4",
#               "comparison_name": "dob",
#               "comparison_vector_value": 4,
#               "label_for_charts": "Damerau_levenshtein <= 1",
#               "log2_bayes_factor": 6.038836762842662,
#               "m_probability": 0.10806341031654734,
#               "sql_condition": "damerau_levenshtein(\"dob_l\", \"dob_r\") <= 1",
#               "u_probability": 0.0016436436436436436
#             },
#             {
#               "bayes_factor": 29.476860590690453,
#               "cl_id": "dob_3",
#               "comparison_name": "dob",
#               "comparison_vector_value": 3,
#               "label_for_charts": "Within 1 month",
#               "log2_bayes_factor": 4.881510974428093,
#               "m_probability": 0.11300938544779224,
#               "sql_condition": "\n            abs(date_diff('month',\n                strptime(\"dob_l\", '%Y-%m-%d'),\n                strptime(\"dob_r\", '%Y-%m-%d'))\n                ) <= 1\n        ",
#               "u_probability": 0.003833833833833834
#             },
#             {
#               "bayes_factor": 3.397551460259144,
#               "cl_id": "dob_2",
#               "comparison_name": "dob",
#               "comparison_vector_value": 2,
#               "label_for_charts": "Within 1 year",
#               "log2_bayes_factor": 1.7644954026183992,
#               "m_probability": 0.17200656922328977,
#               "sql_condition": "\n            abs(date_diff('year',\n                strptime(\"dob_l\", '%Y-%m-%d'),\n                strptime(\"dob_r\", '%Y-%m-%d'))\n                ) <= 1\n        ",
#               "u_probability": 0.05062662662662663
#             },
#             {
#               "bayes_factor": 0.6267794172297388,
#               "cl_id": "dob_1",
#               "comparison_name": "dob",
#               "comparison_vector_value": 1,
#               "label_for_charts": "Within 10 years",
#               "log2_bayes_factor": -0.6739702908716182,
#               "m_probability": 0.19035523041792068,
#               "sql_condition": "\n            abs(date_diff('year',\n                strptime(\"dob_l\", '%Y-%m-%d'),\n                strptime(\"dob_r\", '%Y-%m-%d'))\n                ) <= 10\n        ",
#               "u_probability": 0.3037037037037037
#             },
#             {
#               "bayes_factor": 0.004272180302776005,
#               "cl_id": "dob_0",
#               "comparison_name": "dob",
#               "comparison_vector_value": 0,
#               "label_for_charts": "All other comparisons",
#               "log2_bayes_factor": -7.870811748958801,
#               "m_probability": 0.002727549779972325,
#               "sql_condition": "ELSE",
#               "u_probability": 0.6384444444444445
#             },
#             {
#               "bayes_factor": 10.904938885948333,
#               "cl_id": "city_1",
#               "comparison_name": "city",
#               "comparison_vector_value": 1,
#               "label_for_charts": "Exact match",
#               "log2_bayes_factor": 3.4469097796586596,
#               "m_probability": 0.6013808934279701,
#               "sql_condition": "\"city_l\" = \"city_r\"",
#               "u_probability": 0.0551475711801453
#             },
#             {
#               "bayes_factor": 0.42188504195296994,
#               "cl_id": "city_0",
#               "comparison_name": "city",
#               "comparison_vector_value": 0,
#               "label_for_charts": "All other comparisons",
#               "log2_bayes_factor": -1.2450781575619725,
#               "m_probability": 0.3986191065720299,
#               "sql_condition": "ELSE",
#               "u_probability": 0.9448524288198547
#             },
#             {
#               "bayes_factor": 269.6074384240141,
#               "cl_id": "email_2",
#               "comparison_name": "email",
#               "comparison_vector_value": 2,
#               "label_for_charts": "Exact match",
#               "log2_bayes_factor": 8.07471649055784,
#               "m_probability": 0.5914840252879943,
#               "sql_condition": "\"email_l\" = \"email_r\"",
#               "u_probability": 0.0021938713143283602
#             },
#             {
#               "bayes_factor": 222.9721189153553,
#               "cl_id": "email_1",
#               "comparison_name": "email",
#               "comparison_vector_value": 1,
#               "label_for_charts": "Levenshtein <= 2",
#               "log2_bayes_factor": 7.800719512398763,
#               "m_probability": 0.3019669634613132,
#               "sql_condition": "levenshtein(\"email_l\", \"email_r\") <= 2",
#               "u_probability": 0.0013542812658830492
#             },
#             {
#               "bayes_factor": 0.10692840956298139,
#               "cl_id": "email_0",
#               "comparison_name": "email",
#               "comparison_vector_value": 0,
#               "label_for_charts": "All other comparisons",
#               "log2_bayes_factor": -3.225282884575804,
#               "m_probability": 0.10654901125069259,
#               "sql_condition": "ELSE",
#               "u_probability": 0.9964518474197885
#             }
#           ]
#         },
#         "layer": [
#           {
#             "encoding": {
#               "color": {
#                 "field": "comparison_name",
#                 "legend": null,
#                 "type": "nominal"
#               },
#               "tooltip": [
#                 {
#                   "field": "comparison_name",
#                   "type": "nominal"
#                 },
#                 {
#                   "field": "label_for_charts",
#                   "type": "nominal"
#                 },
#                 {
#                   "field": "sql_condition",
#                   "type": "nominal"
#                 },
#                 {
#                   "field": "m_probability",
#                   "type": "quantitative"
#                 },
#                 {
#                   "field": "u_probability",
#                   "type": "quantitative"
#                 },
#                 {
#                   "field": "bayes_factor",
#                   "type": "quantitative"
#                 },
#                 {
#                   "field": "log2_bayes_factor",
#                   "type": "quantitative"
#                 }
#               ],
#               "x": {
#                 "field": "log2_bayes_factor",
#                 "scale": {
#                   "domain": [
#                     -10,
#                     10
#                   ]
#                 },
#                 "title": "Comparison level match weight = log2(m/u)",
#                 "type": "quantitative"
#               },
#               "y": {
#                 "field": "cl_id",
#                 "sort": "-x",
#                 "title": "Comparison level",
#                 "type": "nominal"
#               }
#             },
#             "mark": {
#               "type": "bar"
#             }
#           },
#           {
#             "encoding": {
#               "text": {
#                 "field": "comparison_name",
#                 "type": "nominal"
#               },
#               "tooltip": [
#                 {
#                   "field": "comparison_name",
#                   "type": "nominal"
#                 },
#                 {
#                   "field": "label_for_charts",
#                   "type": "nominal"
#                 },
#                 {
#                   "field": "sql_condition",
#                   "type": "nominal"
#                 },
#                 {
#                   "field": "m_probability",
#                   "type": "quantitative"
#                 },
#                 {
#                   "field": "u_probability",
#                   "type": "quantitative"
#                 },
#                 {
#                   "field": "bayes_factor",
#                   "type": "quantitative"
#                 },
#                 {
#                   "field": "log2_bayes_factor",
#                   "type": "quantitative"
#                 }
#               ],
#               "x": {
#                 "field": "log2_bayes_factor",
#                 "scale": {
#                   "domain": [
#                     -10,
#                     10
#                   ]
#                 },
#                 "title": "Comparison level match weight = log2(m/u)",
#                 "type": "quantitative"
#               },
#               "y": {
#                 "field": "cl_id",
#                 "sort": "-x",
#                 "title": "Comparison level",
#                 "type": "nominal"
#               }
#             },
#             "mark": {
#               "align": "right",
#               "dx": 0,
#               "type": "text"
#             }
#           }
#         ]
#         }
#       ```
#

# %% [markdown]
# ### Edit in Vega-Lite
#
# Opening the JSON from the above chart in [Vega-Lite editor](https://vega.github.io/editor/#/url/vega-lite/N4IgJAzgxgFgpgWwIYgFwhgF0wBwqgegIDc4BzJAOjIEtMYBXAI0poHsDp5kTykBaADZ04JAKyUAHJQAMlAFYQ2AOxAAaEFBUAzGmTShiNOAHcDmlZhrKGbBhAAScPVjQBmGTI1blVm3YgAdRoAE3p3TwBfSI0QpEwUVFBlJAQ4NBA4hP4kADYoSQAWJABOKG0ARiQZKAB2JAAmKBCPKCgqmTgmiorJMRAYzPikCDhMCHMsgTyC4rLK6rrG5tb26q723v7UAG1QJiQATzgIAH1tJChMNgAnNElcylyGksKSktySsVqxEslJbyCU6hDK6G4QTCnFJpU6FdQWBA4JA3GhKZRQ1LpdBgiEYtLwrSI5GolSnUhXW5kpCCBhYwoaQRIJhwIHaSmwZHjDIAUQAHpdMAACZCYWCCnGQ6HpBlsMgNU4HY5nC4Uu6oR6FNy5WoPQqSGRuBoyf4VDQIU44G5sA5MGjCTCHNByMQyXqvColCq5Cq1NySCr0kAQACOQJ8ITo7FU6AAOiAJXi4KdBHHBQBeQVxhNS043OPwhgWq02u10R2oOSeb4lNzfCpuEo-A1uWoyMQxfZHE7nAW3e4NKQ+zViMQ+yQtwoNQHAkKgmjgyWY05uAlsIkotGJucLrfeNdIjek8nXG5UmlYtwMpks87smCciboAAimJuSELgjgpGUECwzmUgoADwZhU8KCLK8qKt2Konmgjwtr0zzPIUYhuGhLxmkW1pMqWDpOpQHq-NWPqNqhFSumIGghmGKgRlYKgZHEaRvh+X5wD+f7WAAFFm864jmKbqJm8Z8YuMJ5iAACUQEgQWWElva5aVjIDRat6moNL6KF6m4HYgFByq9mqtaUIUOoVA0hS5A8OqaQCmhAiC2KiYmpwNKu64kuiUrbvxmIeQeXlknAqpnrSaBTiAjLMqyd4PhkABSyJsKcJjWAA1p+p4QDQCB2sSDqCgAfBmcglGBEEKl2hmqmgEgBv83xFP6hSurkl4gOalrYbain4TIKF-KhBpIc2xqUUGoanOGkYMeg8jJalGVZacOV5YyKIOjxIk7gJcZqMJ2ZLhJ0klYKZVyd1Cllv1rovN6vxqa2ki1BZekGT2tWoD0lANLk-2FPWrYqapYiRVAjmzs5u1LqBe6eZuPnQ35+Lw4Fm7HpSxDUuF31XjFt6nhyNxcugSVWktyiZXA2W5flm2HMVpVSBVcpVUqn2wagbimTWQ25K6mrjj8mFXThfUVrI7UfL6KnfG8bgVGIgbUdNtGzdGIALRTaVUyta302W21HTCgkHbxMPiXGp3M5Il3FuLN2S1W2o-OOfwNEDNaSO91Wc32kuEQ1I7-TLDZiLk05OTtKNJjIAXEoj-nI2J0oIujR4hSeYVYl4UXXrFRP3iTj4gAAgoIgiCmw9A04KhIZz+rOQX7MEB-wA61r6I4eJ7LyqYGXUO71TtyB6ntfA0RRGu1mqvFRU0zfRmvcgAMgAyty9s9bhSmUH8er-crHr6oRDS+xzbdqj0PMqf6-oelWmqmg5M4ZBADA3DmcJo4npJI0GT+Plf6HnRJjU82NzxoEDNFG8bIi7xXQHyAUwp4hig-l-fyMo2YfSvnBKQRpUIvQsvqbUJRIpDx3hLZ0YNWwuknG8QGZEF40WUHRKMGQ4wYL2iAdMwluHHXzBoQsYsR54WdgNf47wfgjjug0SyJQL7QSMvcOQ3payET6MaT2gYIZv3QAImEK4QFBQAYYtODc-5gKzljHGF58ZwLiiXDIL5mLvmTGxDimB-wyUFHDKKlVcEqPVJQdCrx3gyHajqP4L9KHXXEXIeRfcfiqSNF8dqDwWFqzYRrRir53Gfm-L+bx3EuFAKXGbfh5SrZSV8f4kRw9d63U0iOKyll6xekBpZXISiapcz1JQb4EcigCz1GIf0E09HR3MW5BOoCtwGOqRY-cVjgqhUgbjSKsDC7TWLqTEA5MUq62prTdaBVGZnQutglul9gkSAFp7ZCoN-pGg6nEx2CTZBvEFjWP0HozI9CyUvDh81FrHP1nTDaRsymYNNvtKpsKkwnSZudfe294l708ORGQJQBoNG+H6NwZk3g+zUJ2W5X16ymR1JIGekhGy1BQl6KOUNAGItOP4yx8yzFLLmUFcBOc0Av22YTXZiCDlguWjTVakLzkorkHba57NlFfR5ik54qlWx+jvoPeSHzMUujatEh4tZPgoSBerZeGRtZHKlacw2W0YU8PNmynMyLLks2EXqsRmKDSvRkLUay7x-n4oeL0-2aox6aVegrQNJr5GR1ftMpZpx44mKTviRZiK+UYxsRAuxToHE7OJvsiuVca7wBuPXFZoCJhKqCV9Du+9PieG0QGg0AaSii0adQgif16x1kifWSylkLU5KtUgjeW8vWiKaZLP4gaiVNjMjirUukyX6VbsE1SjwdQaMPvqWojLwaQ0YtaU4-R03-2Tpka0ObM7rILagCaIr4FiucUg-kVxUGihgM3ZVfSA61CkC9Bh455EqRrLE71c65CA0JTqZWLUzKBsTarYFc0QBxhCOewSfDsPnokui-Vt0hxHpQxR8jtRFEbobVzXIEhGXPDBtZIlqFfgsrPUwWE97vI3pw0wXjazs4bLpEW0VJbS6uJpgUzxxSfHAT8f+ujAdHgGn+FqQNU9JwvO7VQ0eBFjSRKJeRRWDGzJErHewzDTEZOsSKZxZQ20BPJnhQR7jyLFP1Jg72rFuRNT+a1AF4LWpw14NQC8al1kBa-EibilCHUpmspc8Y9OqyAECaEwK0T7hxNvskxkYI9BrB+OFJYP99at1fUKCBpW5FGyFEnPqGsemMX4Rvp4b2ysUPkNHZNVh1nNYxgAoKUbY3xuCiZBALiWQkwRm0NoLiAByBA5WltqAhDcHAVg0jOdw-tYbE2jtjaWwAUgAJr8FOwgK7IQluSQ25gLbO24B7Y8wdkbx2Jtncu9d2793JLSS84d47xGfW3UJZD8cUOiVheCTzBsMiAwPN+AGXRp70Aufclevjmbb2CZx8J2xUCIt5acfsorMASsVEFMcZEymqtcwqJQQNjWvitX7e7chrWSOB1qEaSJEdeuGnpUeqzuTYyfa+5Npg03ZunHm4tpbdObjrc29t3Kr33OufUCD6XJ2LtXZu6du7D31cvbe7mD7+vxs-aN-9wHQOQJ64m2D2DsgXRIS939L367yUqq5mov6R73T85eL6f4nHMfns5TW0x-G72E+y0+4VBcJN7NLpT6nMhadwGRHWgJODGftzUb6RskHjSvW9LSnn4PA64trGDDwgNqN0vFxOrDUuvtTZm-EObNAFvLZV2rp7Gvdva7Ni7-Xdu-sm-u4957mvLcSTUFP6XM-jem8B3UmQa-Rtu97R4FsfqT-H5bHDr6ylJz896B26NAs2xR-x6moTGXE9pfmcnknedX3k9LmW6uWuKtLlLyAvcCIvClLmfgYDF6Y0HoRlelcZU+Wvd3TwTSFpN4EPRsVJCadDS1EFEANeTeA-AzDJRrcgig8g9sWjYva+MqAab2RqV4aHRLDHTQMsDlV-G9KAG6JPPNQVPGfOAmfLDPHkL9IUEUWABnSAgOHmRrFtBlD4cZBjD4FA3tAWesfUelIlTScvF+PA8dAguMHgh0HXfDdg0wojGdHtAzNsUcMyOsW-AMVCC-QPUyT2RqAaM+D4d4dHfRCww4F-QnABEwx0Pgx9b-MnBBD9cuSuQAytatBGFQMAwJWgtAfgZnSyF0cyIZb0bAiad5OvOQBsB4D0ciBjfnSDLtfrbJQbHkKdEgz5V4MZEdE0IaMyVwgOP6EoJ4ANIWBhQWF+JLDIRAJAO0WZYIm9UYu0LLfgnLUnIQxxaI-ZZBb9SQirQvG5APAOaQPoqvb5EcHUXVWdXtVHPUDnMGF6XwjqAwuo2MEAaYoEPDDMOMR4q3EARo31CeP0V6RWDSccB5TotUJJfeG-N0JWRvVg-wt42PJI3HLEB45AGY8IkTFPKI99fZVeOTRzXxbHTYgDCNNAGAzwV6L4CyEoxdNQgzDwD0UOWedRYddvAgwpdieTUpREsYp4tzDk8YzzDMPEhpfTT5LFWsJrCyBjDTBg8+GgmQyNQzT4bTHFCOF4f0BsJ-N4tND-ePPHR42YiI3GH-NPEQ8VAAitOuEAtEFIiA7YtUfgHmeRS4qeIoIZfUY4mwz5co5WXFHofFOLfFao24iXQgho6woUved4fzUcIoRlD0GldsAAXUGEZGODVD2AeOUC0AjGUH0CSAsHAjVFAF0BZFZQtOvVRiinIHYihhsErg0AdBwAROUDXGsGpAGDrLYDYEECsBwDQDTKLMEBLLjwzTTnrMbObJSEEDbMLOMAHIyF-2WIL1HIyCbLygnKnJEmLPfkXnwIYjrMOAbOXPHNbL0n7NZUKKaT3IPPQGDAYCQF8DoHiBoFIHXNPIyEFLa0vIRJvLvKsASCsGfJPJnNZRUzuE-IyG-PvL-KfPSEAs3PQHAK2MA1ApACXOvNvMgsfIAvjI0F5HMFfPgtSNlPhGgGpCxFABwyROjB2AyK8HIkTLrLoE-AyAAGEhyVBBQWSq51jBQTBnAyAsA+EEKuIEACAGBJJ4RUKQAILfzMKYKNByxpy4Kk1ZwqJbhMAMh+BcKGLMAmL0BWK4SOK2JJywL0AVyWzJzogzRkR0pzBJKDg7hLLQB2JMzrAczQBvFeR1Lcz8KtThyJL9yxzVzjz2zOzuzezFLZz0BSz4T-KryQAzK1zYLIrFji1RCTL4qjyLKN0fLAyrV0qErgqIqzyfNeCUKArwL0KZL-y5Kiq3ySq8J0rpKHzqqXygKMgQLYqvzKrmroLWqlKEKCS8FGruqoKsKcK8K2qCLrSkLiKoBSLJg1wxiqKaK1A6LBhfzdKQB9LG5DLSAuK0EYAeK+KBKMwhKRKxLOqKqfyeqAL5KJqlLhjVKSYNKtKULGKETtqrFdqWRLrTLMqBhBhkAbgbLczqQ9BNYUR+L1LYhcKKx0qPL1LohEygA), it is now behaving as intended, with both bar and text layers sorted by match weight.
#
# If the chart is working as intended, there is only one step required before saving the JSON file - removing data from the template schema.
#
# The data appears as follows with a dictionary of all included `datasets` by name, and then each chart referencing the `data` it uses by name:
#
# ```
# "data": {"name": "data-a6c84a9cf1a0c7a2cd30cc1a0e2c1185"},
# "datasets": {
#   "data-a6c84a9cf1a0c7a2cd30cc1a0e2c1185": [
#
#     ...
#
#   ]
# },
# ```
#
# Where only one dataset is required, this is equivalent to:
# ```
# "data": {"values": [...]}
# ```
#
# After removing the data references, the template can be saved in Splink as `splink/files/chart_defs/my_new_chart.json`

# %% [markdown]
# ## Combine the chart dataset and template
#
# Putting all of the above together, Splink needs definitions for the methods that generate the chart and the data behind it (these can be separate or performed by the same function if relatively simple).
#
# ### Chart definition
#
# In [`splink/charts.py`](https://github.com/moj-analytical-services/splink/blob/master/splink/charts.py) we can add a new function to populate the chart definition with the provided data:
#
# ```python
# def my_new_chart(records, as_dict=False):
#     chart_path = "my_new_chart.json"
#     chart = load_chart_definition(chart_path)
#
#     chart["data"]["values"] = records
#     return altair_or_json(chart, as_dict=as_dict)
# ```
#
# >**Note** - only the data is being added to a fixed chart definition here. Other elements of the chart spec can be changed by editing the `chart` dictionary in the same way. 
# >
# > For example, if you wanted to add a `color_scheme` argument to replace the default scheme ("tableau10"), this function could include the line: `chart["layer"][0]["encoding"]["color"]["scale"]["scheme"] = color_scheme`
#
# ### Chart method
#
# Then we can add a method to the linker in [`splink/linker.py`](https://github.com/moj-analytical-services/splink/blob/master/splink/linker.py) so the chart can be generated by `linker.my_new_chart()`:
#
# ```python
# from .charts import my_new_chart
#
# ...
#
# class Linker:
#
#     ...
#
#     def my_new_chart(self):
#         
#         # Take linker object and extract complete settings dict
#         records = self._settings_obj._parameters_as_detailed_records
#
#         cols_to_keep = [
#             "comparison_name",
#             "sql_condition",
#             "label_for_charts",
#             "m_probability",
#             "u_probability",
#             "bayes_factor",
#             "log2_bayes_factor",
#             "comparison_vector_value"
#         ]
#
#         # Keep useful information for a match weights chart
#         records = [{k: r[k] for k in cols_to_keep}
#                    for r in records 
#                    if r["comparison_vector_value"] != -1 and r["comparison_sort_order"] != -1]
#
#         return my_new_chart(records)
#
# ```
#
#
# ## Previous new chart PRs
#
# Real-life Splink chart additions, for reference:
#
# - [Term frequency adjustment chart](https://github.com/moj-analytical-services/splink/pull/1226)
# - [Completeness (multi-dataset) chart](https://github.com/moj-analytical-services/splink/pull/669)
# - [Cumulative blocking rule chart](https://github.com/moj-analytical-services/splink/pull/660)
# - [Unlinkables chart](https://github.com/moj-analytical-services/splink/pull/277)
# - [Missingness chart](https://github.com/moj-analytical-services/splink/pull/277)
# - [Waterfall chart](https://github.com/moj-analytical-services/splink/pull/181)
