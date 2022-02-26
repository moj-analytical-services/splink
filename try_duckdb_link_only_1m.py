from splink.duckdb.duckdb_linker import DuckDBInMemoryLinker
from try_settings import settings_dict
import pandas as pd


from splink.duckdb.duckdb_linker import DuckDBInMemoryLinker

from splink.spark.spark_linker import SparkLinker
import pandas as pd


full_name_cc = {
    "column_name": "full_name",
    "comparison_levels": [
        {
            "sql_condition": "full_name_l IS NULL OR full_name_r IS NULL or length(full_name_l) < 2 or length(full_name_r) < 2",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "full_name_l = full_name_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.7,
            "u_probability": 0.1,
            "tf_adjustment_column": "full_name",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "levenshtein(full_name_l, full_name_r) <= 2",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "Levenstein <= 2",
        },
        {
            "sql_condition": "levenshtein(full_name_l, full_name_r) <= 4",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "Levenstein <= 4",
        },
        {
            "sql_condition": "levenshtein(full_name_l, full_name_r) <= 8",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "Levenstein <= 8",
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.8,
        },
    ],
}


dob_cc = {
    "column_name": "dob",
    "comparison_levels": [
        {
            "sql_condition": "dob_l IS NULL OR dob_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "dob_l = dob_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

birth_place_cc = {
    "column_name": "birth_place",
    "comparison_levels": [
        {
            "sql_condition": "birth_place_l IS NULL OR birth_place_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "birth_place_l = birth_place_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

postcode_cc = {
    "column_name": "postcode",
    "comparison_levels": [
        {
            "sql_condition": "postcode_l IS NULL OR postcode_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "postcode_l = postcode_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
            "tf_adjustment_column": "postcode",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}


occupation_cc = {
    "column_name": "occupation",
    "comparison_levels": [
        {
            "sql_condition": "occupation_l IS NULL OR occupation_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "occupation_l = occupation_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
            "tf_adjustment_column": "occupation",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

settings_dict = {
    "proportion_of_matches": 0.01,
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.postcode = r.postcode and substr(l.full_name,1,2) = substr(r.full_name,1,2)",
        "l.dob = r.dob and substr(l.postcode,1,2) = substr(r.postcode,1,2)",
        "l.postcode = r.postcode and substr(l.dob,1,3) = substr(r.dob,1,3)",
        "l.postcode = r.postcode and substr(l.dob,4,5) = substr(r.dob,4,5)",
    ],
    "comparisons": [
        full_name_cc,
        dob_cc,
        birth_place_cc,
        postcode_cc,
        occupation_cc,
    ],
    "retain_matching_columns": False,
    "retain_intermediate_calculation_columns": False,
    "additional_columns_to_retain": ["cluster"],
    "max_iterations": 10,
}


df = pd.read_parquet("./benchmarking/synthetic_data_all.parquet")


df_main = df
df_new = df[:10].copy()

linker = DuckDBInMemoryLinker(settings_dict, input_tables={"main": df_main})


# Train it as a dedupe job.
# If you were to do that, the left hand table would be '__splink__df_concat_with_tf'

# It needs a 'link_incremental' method that treats ''__splink__df_concat_with_tf'' as the left
# table and 'main' as the right table of a link_only.


# linker.list_tables()
linker.train_u_using_random_sampling(target_rows=1e6)

linker.train_m_using_expectation_maximisation("l.full_name = r.full_name")

linker.train_m_using_expectation_maximisation(
    "l.dob = r.dob and substr(l.postcode,1,2) = substr(r.postcode,1,2)"
)
df = linker.predict()


df_pd = df.as_pandas_dataframe()


df_new["source_dataset"] = "df_new"


linker.con.register("__splink__new", df_new)


linker.compute_tf_table("full_name")
linker.compute_tf_table("postcode")
linker.compute_tf_table("occupation")

import time

start_time = time.time()

df = linker.incremental_link("__splink__new").as_pandas_dataframe()
df
print("--- %s seconds ---" % (time.time() - start_time))
df
