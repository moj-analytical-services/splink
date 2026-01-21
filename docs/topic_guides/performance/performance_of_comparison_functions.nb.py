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

# %% tags=["hide_input", "hide_output"]
import urllib.request

import pandas as pd
import json
pd.options.display.max_colwidth = 10009

url = (
    "https://raw.githubusercontent.com/moj-analytical-services/splink_speed_testing/"
    "39e235d2452793fd59a11da4a813642013404ed0/.benchmarks/Darwin-CPython-3.11-64bit/"
    "0006_b5b7ee569dab10ff304d1123984a2f446917fe9e_20241205_124128.json"
)
with urllib.request.urlopen(url) as u:
    data_text = u.read().decode()

data = json.loads(data_text)
# Extract the benchmark statistics
benchmarks = data['benchmarks']

# Create a DataFrame from the benchmark statistics
df = pd.DataFrame([{
    'name': benchmark['name'],
    'rounds': benchmark['stats']['rounds'],
    'median': benchmark['stats']['median'],

    'iterations': benchmark['stats']['iterations']
} for benchmark in benchmarks])

df['comparison_type'] = df['name'].str.extract(r'\[(.*?)-(?:duckdb|spark)\]')[0]
df['backend'] = df['name'].str.extract(r'-(duckdb|spark)\]')[0]


df['name'] = df['name'].str.replace(r'test_comparison_execution_\w+\[.*?\]', '', regex=True)

df = df.drop('name', axis=1)

# Get exact match times for each backend
exact_match_times = df[df['comparison_type'] == 'Exact Match'].set_index('backend')['median']

# Calculate multiples using merge and division
df['multiple_of_exact_match'] = df.apply(
    lambda x: x['median'] / exact_match_times[x['backend']],
    axis=1
)

df['comparison_type'] = df['comparison_type'].apply(
    lambda x: f"{x}*" if x == 'Cosine Similarity Level' else x
)




# %% tags=["hide_input", "hide_output"]
import altair as alt



# Create base chart function to avoid code duplication
def create_runtime_chart(data, backend_name):
    return alt.Chart(data).mark_bar().encode(
        y=alt.Y('comparison_type:N',
                sort=alt.EncodingSortField(field='median', order='ascending'),
                title='Comparison Type'),
        x=alt.X('median:Q',
                title='Median Runtime (seconds)'),
        tooltip=[
            alt.Tooltip('comparison_type', title='Comparison'),
            alt.Tooltip('median', title='Runtime (s)', format='.3f'),
            alt.Tooltip('multiple_of_exact_match', title='Times slower than exact match', format='.1f'),
            alt.Tooltip('rounds', title='Rounds'),
        ]
    ).properties(
        title=f'{backend_name} Comparison Runtimes',
        height=400
    )

# Create charts for both backends
duckdb_df = df[df['backend'] == 'duckdb'].sort_values('median')
spark_df = df[df['backend'] == 'spark'].sort_values('median')

duckdb_chart = create_runtime_chart(duckdb_df, 'DuckDB')
spark_chart = create_runtime_chart(spark_df, 'Spark')


# %% [markdown]
# ### Comparing the execution speed of different comparisons 
#
# An important determinant of Splink performance is the computational complexity of any similarity or distance measures (fuzzy matching functions) used as part of your [model config](https://moj-analytical-services.github.io/splink/topic_guides/comparisons/).
#
# For example, you may be considering using [Jaro Winkler](https://moj-analytical-services.github.io/splink/topic_guides/comparisons/comparators.html?h=levenshtein#jaro-winkler-similarity)  or [Levenshtein](https://moj-analytical-services.github.io/splink/topic_guides/comparisons/comparators.html?h=levenshtein#levenshtein-distance), and wish to know which will take longer to compute.
#
# This page contains summary statistics from performance benchmarking these functions.  The code used to generate these results can be found [here](https://github.com/moj-analytical-services/splink_speed_testing/), and raw results can be found [here](https://github.com/moj-analytical-services/splink_speed_testing/tree/main/.benchmarks/Darwin-CPython-3.11-64bit).
#
# The timings are based on making 10,000,000 comparisons of the named function.
#

# %% [markdown]
# #### DuckDB
#
# The following chart shows the performance of different functions in DuckDB

# %% tags=["hide_input"]
duckdb_chart

# %% [markdown]
# #### Spark
#
# The following chart shows the performance of different functions in Spark
#
#

# %% tags=["hide_input"]
spark_chart

# %% [markdown]
# #### Caveats and notes
#
# These charts are intended to provide a rough, high level guide to performance.  Real world performance can be sensitive to a number of factors:
#
# - For some functions such as Levenshtein, a longer input string will take longer to compute.
# - For some functions, it may be simpler to compute the result when comparing two similar strings
# - For the cosine similarity function, we used an embeddings length of 10.  This is far lower than many typical applications e.g. OpenAI's can have a length of [1,536](https://openai.com/index/new-embedding-models-and-api-updates/).  The reason was that we were running out of memory (RAM) for longer lengths, causing spill to disk, which in turn prevented the test being a pure test of the function itself.
#
# If you wish to run your own benchmarks, head over to the [splink_speed_testing](https://github.com/moj-analytical-services/splink_speed_testing/) repo, create tests [like these](https://github.com/moj-analytical-services/splink_speed_testing/blob/main/benchmarks/test_comparison_levels.py) and then run using the command 
#
# ```bash
# pytest benchmarks/
# ```
#
#
