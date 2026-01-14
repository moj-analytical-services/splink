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
# # `XXXXX_chart`
#
# !!! info "At a glance"
#     **Useful for:** 
#
#     **API Documentation:** [XXXXXX_chart()](http://example.com)
#
#     **What is needed to generate the chart?** 

# %% [markdown]
# ## Worked Example

# %%
from splink.duckdb.linker import DuckDBLinker
import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl
from splink.duckdb.blocking_rule_library import block_on
from splink.datasets import splink_datasets
import logging, sys
logging.disable(sys.maxsize)

df = splink_datasets.fake_1000

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        block_on("first_name"),
        block_on("surname"),
    ],
    "comparisons": [
        ctl.name_comparison("first_name"),
        ctl.name_comparison("surname"),
        ctl.date_comparison("dob", cast_strings_to_date=True),
        cl.exact_match("city", term_frequency_adjustments=True),
        ctl.email_comparison("email", include_username_fuzzy_level=False),
    ],
}

linker = DuckDBLinker(df, settings)
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

blocking_rule_for_training = block_on(["first_name", "surname"])

linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

blocking_rule_for_training = block_on("dob")
linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)



# %% [markdown]
# ### What the chart shows
#

# %% [markdown]
# ??? note "What the chart tooltip shows"
#
#     ![]()

# %% [markdown]
# ### How to interpret the chart
#

# %% [markdown]
# ### Actions to take as a result of the chart
#
#
