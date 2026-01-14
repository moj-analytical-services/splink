# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Evaluation of prediction results
#
#  <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/07_Quality_assurance.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# In the previous tutorial, we looked at various ways to visualise the results of our model.
# These are useful for evaluating a linkage pipeline because they allow us to understand how our model works and verify that it is doing something sensible. They can also be useful to identify examples where the model is not performing as expected.
#
# In addition to these spot checks, Splink also has functions to perform more formal accuracy analysis. These functions allow you to understand the likely prevalence of false positives and false negatives in your linkage models.
#
# They rely on the existence of a sample of labelled (ground truth) matches, which may have been produced (for example) by human beings. For the accuracy analysis to be unbiased, the sample should be representative of the overall dataset.
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
# Rerun our predictions to we're ready to view the charts
import pandas as pd

from splink import DuckDBAPI, Linker, splink_datasets

pd.options.display.max_columns = 1000

db_api = DuckDBAPI()
df = splink_datasets.fake_1000
df_sdf = db_api.register(df)

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

from splink import block_on


settings = json.loads(get_settings_text())

# The data quality is very poor in this dataset, so we need looser blocking rules
# to achieve decent recall
settings["blocking_rules_to_generate_predictions"] = [
    block_on("first_name"),
    block_on("city"),
    block_on("email"),
    block_on("dob"),
]

linker = Linker(df_sdf, settings)
df_predictions = linker.inference.predict(threshold_match_probability=0.01)

# %% [markdown]
# ## Load in labels
#
# The labels file contains a list of pairwise comparisons which represent matches and non-matches.
#
# The required format of the labels file is described [here](https://moj-analytical-services.github.io/splink/api_docs/evaluation.html#splink.internals.linker_components.evaluation.LinkerEvalution.prediction_errors_from_labels_table).
#

# %%
from splink.datasets import splink_dataset_labels

df_labels = splink_dataset_labels.fake_1000_labels
labels_table = linker.table_management.register_labels_table(df_labels)
df_labels.head(5)

# %% [markdown]
# ## View examples of false positives and false negatives

# %%
splink_df = linker.evaluation.prediction_errors_from_labels_table(
    labels_table, include_false_negatives=True, include_false_positives=False
)
false_negatives = splink_df.as_record_dict(limit=5)
linker.visualisations.waterfall_chart(false_negatives)

# %% [markdown]
# ### False positives

# %%
# Note I've picked a threshold match probability of 0.01 here because otherwise
# in this simple example there are no false positives
splink_df = linker.evaluation.prediction_errors_from_labels_table(
    labels_table, include_false_negatives=False, include_false_positives=True, threshold_match_probability=0.01
)
false_postives = splink_df.as_record_dict(limit=5)
linker.visualisations.waterfall_chart(false_postives)

# %% [markdown]
# ## Threshold Selection chart
#
# Splink includes an interactive dashboard that shows key accuracy statistics:
#

# %%
linker.evaluation.accuracy_analysis_from_labels_table(
    labels_table, output_type="threshold_selection", add_metrics=["f1"]
)

# %% [markdown]
# ## Receiver operating characteristic curve
#
# A [ROC chart](https://en.wikipedia.org/wiki/Receiver_operating_characteristic) shows how the number of false positives and false negatives varies depending on the match threshold chosen. The match threshold is the match weight chosen as a cutoff for which pairwise comparisons to accept as matches.
#

# %%
linker.evaluation.accuracy_analysis_from_labels_table(labels_table, output_type="roc")

# %% [markdown]
# ## Truth table
#
# Finally, Splink can also report the underlying table used to construct the ROC and precision recall curves.
#

# %%
roc_table = linker.evaluation.accuracy_analysis_from_labels_table(
    labels_table, output_type="table"
)
roc_table.as_pandas_dataframe(limit=5)

# %% [markdown]
# ## Unlinkables chart
#
# Finally, it can be interesting to analyse whether your dataset contains any 'unlinkable' records.
#
# 'Unlinkable records' are records with such poor data quality they don't even link to themselves at a high enough probability to be accepted as matches
#
# For example, in a typical linkage problem, a 'John Smith' record with nulls for their address and postcode may be unlinkable.  By 'unlinkable' we don't mean there are no matches; rather, we mean it is not possible to determine whether there are matches.UnicodeTranslateError
#
# A high proportion of unlinkable records is an indication of poor quality in the input dataset

# %%
linker.evaluation.unlinkables_chart()

# %% [markdown]
# For this dataset and this trained model, we can see that most records are (theoretically) linkable:  At a match weight 6, around around 99% of records could be linked to themselves.

# %% [markdown]
# !!! note "Further Reading"
#
#     :material-tools: For more on the quality assurance tools in Splink, please refer to the [Evaluation API documentation](../../api_docs/evaluation.md).
#
#     :bar_chart: For more on the charts used in this tutorial, please refer to the [Charts Gallery](../../charts/index.md#model-evaluation).
#
#     :material-thumbs-up-down: For more on the Evaluation Metrics used in this tutorial, please refer to the [Edge Metrics guide.](../../topic_guides/evaluation/edge_metrics.md)
#

# %% [markdown]
# ## :material-flag-checkered: That's it!
#
# That wraps up the Splink tutorial! Don't worry, there are still plenty of resources to help on the next steps of your Splink journey:
#
# :octicons-link-16: For some end-to-end notebooks of Splink pipelines, check out our [Examples](../examples/examples_index.md)
#
# :simple-readme: For more deepdives into the different aspects of Splink, and record linkage more generally, check out our [Topic Guides](../../topic_guides/topic_guides_index.md)
#
# :material-tools: For a reference on all the functionality avalable in Splink, see our [Documentation](../../api_docs/api_docs_index.md)
#
