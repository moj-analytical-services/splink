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
# ## Linking banking transactions
#
# This example shows how to perform a one-to-one link on banking transactions.
#
# The data is fake data, and was generated has the following features:
#
# - Money shows up in the destination account with some time delay
# - The amount sent and the amount received are not always the same - there are hidden fees and foreign exchange effects
# - The memo is sometimes truncated and content is sometimes missing
#
# Since each origin payment should end up in the destination account, the `probability_two_random_records_match` of the model is known.
#

# %% [markdown]
# <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/transactions.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install splink

# %%
from IPython.display import display

from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

df_origin = splink_datasets.transactions_origin
df_destination = splink_datasets.transactions_destination

display(df_origin.head(2))
display(df_destination.head(2))

# %% [markdown]
# In the following chart, we can see this is a challenging dataset to link:
#
# - There are only 151 distinct transaction dates, with strong skew
# - Some 'memos' are used multiple times (up to 48 times)
# - There is strong skew in the 'amount' column, with 1,400 transactions of around 60.00
#

# %%
from splink.exploratory import profile_columns

db_api = DuckDBAPI()
df_origin_sdf = db_api.register(df_origin)
df_destination_sdf = db_api.register(df_destination)
profile_columns(
    [df_origin_sdf, df_destination_sdf],
    column_expressions=[
        "memo",
        "transaction_date",
        "amount",
    ],
)

# %%
from splink import DuckDBAPI, block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)

# Design blocking rules that allow for differences in transaction date and amounts
blocking_rule_date_1 = """
    strftime(l.transaction_date, '%Y%m') = strftime(r.transaction_date, '%Y%m')
    and substr(l.memo, 1,3) = substr(r.memo,1,3)
    and l.amount/r.amount > 0.7   and l.amount/r.amount < 1.3
"""

# Offset by half a month to ensure we capture case when the dates are e.g. 31st Jan and 1st Feb
blocking_rule_date_2 = """
    strftime(l.transaction_date+15, '%Y%m') = strftime(r.transaction_date, '%Y%m')
    and substr(l.memo, 1,3) = substr(r.memo,1,3)
    and l.amount/r.amount > 0.7   and l.amount/r.amount < 1.3
"""

blocking_rule_memo = block_on("substr(memo,1,9)")

blocking_rule_amount_1 = """
round(l.amount/2,0)*2 = round(r.amount/2,0)*2 and yearweek(r.transaction_date) = yearweek(l.transaction_date)
"""

blocking_rule_amount_2 = """
round(l.amount/2,0)*2 = round((r.amount+1)/2,0)*2 and yearweek(r.transaction_date) = yearweek(l.transaction_date + 4)
"""

blocking_rule_cheat = block_on("unique_id")


brs = [
    blocking_rule_date_1,
    blocking_rule_date_2,
    blocking_rule_memo,
    blocking_rule_amount_1,
    blocking_rule_amount_2,
    blocking_rule_cheat,
]


db_api = DuckDBAPI()
df_origin_sdf = db_api.register(df_origin)
df_destination_sdf = db_api.register(df_destination)

cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    [df_origin_sdf, df_destination_sdf],
    blocking_rules=brs,
    link_type="link_only"
)

# %%
# Full settings for linking model
import splink.comparison_level_library as cll
import splink.comparison_library as cl

comparison_amount = {
    "output_column_name": "amount",
    "comparison_levels": [
        cll.NullLevel("amount"),
        cll.ExactMatchLevel("amount"),
        cll.PercentageDifferenceLevel("amount", 0.01),
        cll.PercentageDifferenceLevel("amount", 0.03),
        cll.PercentageDifferenceLevel("amount", 0.1),
        cll.PercentageDifferenceLevel("amount", 0.3),
        cll.ElseLevel(),
    ],
    "comparison_description": "Amount percentage difference",
}

# The date distance is one sided becaause transactions should only arrive after they've left
# As a result, the comparison_template_library date difference functions are not appropriate
within_n_days_template = "transaction_date_r - transaction_date_l <= {n} and transaction_date_r >= transaction_date_l"

comparison_date = {
    "output_column_name": "transaction_date",
    "comparison_levels": [
        cll.NullLevel("transaction_date"),
        {
            "sql_condition": within_n_days_template.format(n=1),
            "label_for_charts": "1 day",
        },
        {
            "sql_condition": within_n_days_template.format(n=4),
            "label_for_charts": "<=4 days",
        },
        {
            "sql_condition": within_n_days_template.format(n=10),
            "label_for_charts": "<=10 days",
        },
        {
            "sql_condition": within_n_days_template.format(n=30),
            "label_for_charts": "<=30 days",
        },
        cll.ElseLevel(),
    ],
    "comparison_description": "Transaction date days apart",
}


settings = SettingsCreator(
    link_type="link_only",
    probability_two_random_records_match=1 / len(df_origin),
    blocking_rules_to_generate_predictions=[
        blocking_rule_date_1,
        blocking_rule_date_2,
        blocking_rule_memo,
        blocking_rule_amount_1,
        blocking_rule_amount_2,
        blocking_rule_cheat,
    ],
    comparisons=[
        comparison_amount,
        cl.LevenshteinAtThresholds("memo", [2, 6, 10]),
        comparison_date,
    ],
    retain_intermediate_calculation_columns=True,
)

# %%
db_api = DuckDBAPI()
df_origin_sdf = db_api.register(df_origin, source_dataset_name="__ori")
df_destination_sdf = db_api.register(df_destination, source_dataset_name="_dest")
linker = Linker([df_origin_sdf, df_destination_sdf], settings)

# %%
linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

# %%
linker.training.estimate_parameters_using_expectation_maximisation(block_on("memo"))

# %%
session = linker.training.estimate_parameters_using_expectation_maximisation(block_on("amount"))

# %%
linker.visualisations.match_weights_chart()

# %%
df_predict = linker.inference.predict(threshold_match_probability=0.001)

# %%
linker.visualisations.comparison_viewer_dashboard(
    df_predict, "dashboards/comparison_viewer_transactions.html", overwrite=True
)
from IPython.display import IFrame

IFrame(
    src="./dashboards/comparison_viewer_transactions.html", width="100%", height=1200
)

# %%
pred_errors = linker.evaluation.prediction_errors_from_labels_column(
    "ground_truth", include_false_positives=True, include_false_negatives=False
)
linker.visualisations.waterfall_chart(pred_errors.as_record_dict(limit=5))

# %%
pred_errors = linker.evaluation.prediction_errors_from_labels_column(
    "ground_truth", include_false_positives=False, include_false_negatives=True
)
linker.visualisations.waterfall_chart(pred_errors.as_record_dict(limit=5))
