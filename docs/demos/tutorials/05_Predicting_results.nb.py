# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: splink (3.10.18)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Predicting which records match
#
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/05_Predicting_results.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# In the previous tutorial, we built and estimated a linkage model.
#
# In this tutorial, we will load the estimated model and use it to make predictions of which pairwise record comparisons match.
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
from splink import Linker, DuckDBAPI, splink_datasets

import pandas as pd

pd.options.display.max_columns = 1000

db_api = DuckDBAPI()
df = splink_datasets.fake_1000
df_sdf = db_api.register(df)

# %% [markdown]
# ## Load estimated model from previous tutorial
#

# %%
import urllib.request
from pathlib import Path


def get_settings_text() -> str:
    # assumes cwd is folder of this notebook
    local_path = Path.cwd() / ".." / "demo_settings" / "saved_model_from_demo.json"

    if local_path.exists():
        return local_path.read_text()

    # fallback location for settings - the file as it is on master, for e.g. colab use
    url = "https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/demos/demo_settings/saved_model_from_demo.json"
    with urllib.request.urlopen(url) as u:
        return u.read().decode()



# %%
import json

settings = json.loads(get_settings_text())

linker = Linker(df_sdf, settings)

# %% [markdown]
# # Predicting match weights using the trained model
#
# We use `linker.inference.predict()` to run the model.
#
# Under the hood this will:
#
# - Generate all pairwise record comparisons that match at least one of the `blocking_rules_to_generate_predictions`
#
# - Use the rules specified in the `Comparisons` to evaluate the similarity of the input data
#
# - Use the estimated match weights, applying term frequency adjustments where requested to produce the final `match_weight` and `match_probability` scores
#
# Optionally, a `threshold_match_probability` or `threshold_match_weight` can be provided, which will drop any row where the predicted score is below the threshold.
#

# %%
df_predictions = linker.inference.predict(threshold_match_probability=0.2)
df_predictions.as_pandas_dataframe(limit=5)

# %% [markdown]
# ## Clustering
#
# The result of `linker.inference.predict()` is a list of pairwise record comparisons and their associated scores. For instance, if we have input records A, B, C and D, it could be represented conceptually as:
#
# ```
# A -> B with score 0.9
# B -> C with score 0.95
# C -> D with score 0.1
# D -> E with score 0.99
# ```
#
# Often, an alternative representation of this result is more useful, where each row is an input record, and where records link, they are assigned to the same cluster.
#
# With a score threshold of 0.5, the above data could be represented conceptually as:
#
# ```
# ID, Cluster ID
# A,  1
# B,  1
# C,  1
# D,  2
# E,  2
# ```
#
# The algorithm that converts between the pairwise results and the clusters is called connected components, and it is included in Splink. You can use it as follows:
#

# %%
clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    df_predictions, threshold_match_probability=0.5
)
clusters.as_pandas_dataframe(limit=10)

# %%
sql = f"""
select *
from {df_predictions.physical_name}
limit 2
"""
linker.misc.query_sql(sql)

# %% [markdown]
# !!! note "Further Reading"
# :material-tools: For more on the prediction tools in Splink, please refer to the [Prediction API documentation](../../api_docs/inference.md).
#

# %% [markdown]
# ## Next steps
#
# Now we have made predictions with a model, we can move on to visualising it to understand how it is working.
#
