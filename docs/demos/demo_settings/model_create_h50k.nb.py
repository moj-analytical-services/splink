# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# #!conda install -c conda-forge splink=4.0 --yes

# %%
from splink import splink_datasets

df = splink_datasets.historical_50k

# %%
from splink import DuckDBAPI
db_api = DuckDBAPI()

# %%
from splink import DuckDBAPI, block_on

blocking_rules = [
    block_on("substr(first_name,1,3)", "substr(surname,1,4)"),
    block_on("surname", "dob"),
    block_on("first_name", "dob"),
    block_on("postcode_fake", "first_name"),
    block_on("postcode_fake", "surname"),
    block_on("dob", "birth_place"),
    block_on("substr(postcode_fake,1,3)", "dob"),
    block_on("substr(postcode_fake,1,3)", "first_name"),
    block_on("substr(postcode_fake,1,3)", "surname"),
    block_on("substr(first_name,1,2)", "substr(surname,1,2)", "substr(dob,1,4)"),
]

# %%
import splink.comparison_library as cl

from splink import Linker, SettingsCreator

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=blocking_rules,
    comparisons=[
        cl.NameComparison("first_name").configure(term_frequency_adjustments=False),
        cl.NameComparison("surname").configure(term_frequency_adjustments=False),
        cl.DateOfBirthComparison("dob", input_is_string=True),
        cl.PostcodeComparison("postcode_fake"),
        cl.ExactMatch("birth_place").configure(term_frequency_adjustments=False),
        cl.ExactMatch("occupation").configure(term_frequency_adjustments=False),
    ],
    retain_intermediate_calculation_columns=True,
)

df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)

# %%
linker.training.estimate_probability_two_random_records_match(
    [
        "l.first_name = r.first_name and l.surname = r.surname and l.dob = r.dob",
        "substr(l.first_name,1,2) = substr(r.first_name,1,2) and l.surname = r.surname and substr(l.postcode_fake,1,2) = substr(r.postcode_fake,1,2)",
        "l.dob = r.dob and l.postcode_fake = r.postcode_fake",
    ],
    recall=0.6,
)

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=5e6)

# %%
training_blocking_rule = block_on("first_name", "surname")
training_session_names = (
    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule, estimate_without_term_frequencies=True
    )
)

# %%
training_blocking_rule = block_on("dob")
training_session_dob = (
    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule, estimate_without_term_frequencies=True
    )
)

# %%
linker.misc.save_model_to_json("model_h50k.json", overwrite=True)
