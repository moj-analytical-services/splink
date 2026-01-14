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
# - the `linker.inference.compare_two_records` function, that allows you to interactively explore the results of a linkage model; and
# - the `linker.find_matches_to_new_records` that allows you to incrementally find matches to a small number of new records
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/real_time_record_linkage.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install ipywidgets
# # !pip install splink
# # !jupyter nbextension enable --py widgetsnbextension

# %% [markdown]
# ### Step 1: Load a pre-trained linkage model
#

# %%
import urllib.request
from pathlib import Path


def get_settings_text() -> str:
    # assumes cwd is folder of this notebook
    local_path = Path.cwd() / ".." / ".." / "demo_settings" / "real_time_settings.json"

    if local_path.exists():
        return local_path.read_text()

    # fallback location for settings - the file as it is on master, for e.g. colab use
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

# %%
linker.visualisations.waterfall_chart(
    linker.inference.predict().as_record_dict(limit=2)
)

# %% [markdown]
# ### Step Comparing two records
#
# It's now possible to compute a match weight for any two records using `linker.inference.compare_two_records()`
#

# %%
record_1 = {
    "unique_id": 1,
    "first_name": "Lucas",
    "surname": "Smith",
    "dob": "1984-01-02",
    "city": "London",
    "email": "lucas.smith@hotmail.com",
}

record_2 = {
    "unique_id": 2,
    "first_name": "Lucas",
    "surname": "Smith",
    "dob": "1983-02-12",
    "city": "Machester",
    "email": "lucas.smith@hotmail.com",
}

linker._settings_obj._retain_intermediate_calculation_columns = True


# To `compare_two_records` the linker needs to compute term frequency tables
# If you have precomputed tables, you can linker.table_management.register_term_frequency_lookup()
linker.table_management.compute_tf_table("first_name")
linker.table_management.compute_tf_table("surname")
linker.table_management.compute_tf_table("dob")
linker.table_management.compute_tf_table("city")
linker.table_management.compute_tf_table("email")


df_two = linker.inference.compare_two_records(record_1, record_2)
df_two.as_pandas_dataframe()

# %% [markdown]
# ### Step 3: Interactive comparisons
#
# One interesting applicatin of `compare_two_records` is to create a simple interface that allows the user to input two records interactively, and get real time feedback.
#
# In the following cell we use `ipywidets` for this purpose. ✨✨ Change the values in the text boxes to see the waterfall chart update in real time. ✨✨
#

# %%
import ipywidgets as widgets
from IPython.display import display


fields = ["unique_id", "first_name", "surname", "dob", "email", "city"]

left_text_boxes = []
right_text_boxes = []

inputs_to_interactive_output = {}

for f in fields:
    wl = widgets.Text(description=f, value=str(record_1[f]))
    left_text_boxes.append(wl)
    inputs_to_interactive_output[f"{f}_l"] = wl
    wr = widgets.Text(description=f, value=str(record_2[f]))
    right_text_boxes.append(wr)
    inputs_to_interactive_output[f"{f}_r"] = wr

b1 = widgets.VBox(left_text_boxes)
b2 = widgets.VBox(right_text_boxes)
ui = widgets.HBox([b1, b2])


def myfn(**kwargs):
    my_args = dict(kwargs)

    record_left = {}
    record_right = {}

    for key, value in my_args.items():
        if value == "":
            value = None
        if key.endswith("_l"):
            record_left[key[:-2]] = value
        elif key.endswith("_r"):
            record_right[key[:-2]] = value

    # Assuming 'linker' is defined earlier in your code
    linker._settings_obj._retain_intermediate_calculation_columns = True

    df_two = linker.inference.compare_two_records(record_left, record_right)

    recs = df_two.as_pandas_dataframe().to_dict(orient="records")

    display(linker.visualisations.waterfall_chart(recs, filter_nulls=False))


out = widgets.interactive_output(myfn, inputs_to_interactive_output)

display(ui, out)

# %% [markdown]
# ## Finding matching records interactively
#
# It is also possible to search the records in the input dataset rapidly using the `linker.find_matches_to_new_records()` function
#

# %%
record = {
    "unique_id": 123987,
    "first_name": "Robert",
    "surname": "Alan",
    "dob": "1971-05-24",
    "city": "London",
    "email": "robert255@smith.net",
}


df_inc = linker.inference.find_matches_to_new_records(
    [record], blocking_rules=[]
).as_pandas_dataframe()
df_inc.sort_values("match_weight", ascending=False)


# %% [markdown]
# ## Interactive interface for finding records
#
# Again, we can use `ipywidgets` to build an interactive interface for the `linker.find_matches_to_new_records` function
#

# %%
@widgets.interact(
    first_name="Robert",
    surname="Alan",
    dob="1971-05-24",
    city="London",
    email="robert255@smith.net",
)
def interactive_link(first_name, surname, dob, city, email):
    record = {
        "unique_id": 123987,
        "first_name": first_name,
        "surname": surname,
        "dob": dob,
        "city": city,
        "email": email,
        "group": 0,
    }

    for key in record.keys():
        if type(record[key]) == str:
            if record[key].strip() == "":
                record[key] = None

    df_inc = linker.inference.find_matches_to_new_records(
        [record], blocking_rules=[f"(true)"]
    ).as_pandas_dataframe()
    df_inc = df_inc.sort_values("match_weight", ascending=False)
    recs = df_inc.to_dict(orient="records")

    display(linker.visualisations.waterfall_chart(recs, filter_nulls=False))


# %%
linker.visualisations.match_weights_chart()
