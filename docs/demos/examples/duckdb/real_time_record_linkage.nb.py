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
# ## Real time linkage
#

# %% [markdown]
# In this notebook, we demonstrate splink's incremental and real time linkage capabilities - specifically:
#
# - the `linker.inference.score_pair` function, that allows you to interactively explore the results of a linkage model
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/real_time_record_linkage.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %% [markdown]
# ### Step 1: Load a pre-trained linkage model
#

# %%
import urllib.request
from pathlib import Path


def get_settings_text() -> str:
    # assumes cwd is repo root
    local_path = Path.cwd() / "docs" / "demos" / "demo_settings" / "real_time_settings.json"

    if local_path.exists():
        return local_path.read_text()

    # fallback location for settings - the file as it is on master, for e.g. colab use
    # TODO: update ref
    url = "https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/demos/demo_settings/real_time_settings.json"
    with urllib.request.urlopen(url) as u:
        return u.read().decode()



# %%
import json

from splink import DuckDBAPI, Linker, splink_datasets

df = splink_datasets.fake_1000

settings = json.loads(get_settings_text())

db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)

# %% [markdown]
# ### Step 2: Comparing two records
#
# It's now possible to compute a match weight for any two records using `linker.inference.score_pair()`
#

# %%
record_1 = {
    "unique_id": 1,
    "first_name": "Lucas",
    "surname": "Smith",
    "dob": "1984-01-02",
    "city": "London",
    "email": "lucas.smith@hotmail.com",
    "tf_first_name": 0.0012,
    "tf_surname": 0.0134,
    "tf_city": 0.21,

}

record_2 = {
    "unique_id": 2,
    "first_name": "Lucas",
    "surname": "Smith",
    "dob": "1983-02-12",
    "city": "Machester",
    "email": "lucas.smith@hotmail.com",
    "tf_first_name": 0.0012,
    "tf_surname": 0.0134,
    "tf_city": 0.01,

}

linker._settings_obj._retain_intermediate_calculation_columns = True


# Term frequency values should be provided for columns that use term frequency
# adjustments. If they are omitted, Splink falls back to registered term frequency
# lookup tables.


df_two = linker.inference.score_pair(record_1, record_2)
df_two.as_duckdbpyrelation().show(max_width=10000)

linker.visualisations.waterfall_chart(df_two.as_record_dict())


# %%
