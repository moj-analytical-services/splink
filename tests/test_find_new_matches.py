from copy import deepcopy

import pandas as pd

from splink.duckdb.comparison_library import exact_match
from splink.duckdb.linker import DuckDBLinker
from tests.basic_settings import get_settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")


settings = get_settings_dict()
settings_tf = deepcopy(settings, None)
# Settings with two term frequency columns
settings_tf["comparisons"][1] = exact_match(
    "surname", True, m_probability_exact_match=0.7, m_probability_else=0.1
)
settings_no_tf = deepcopy(settings, None)
# Settings with no term frequencies
settings_no_tf["comparisons"][0] = exact_match(
    "first_name", False, m_probability_exact_match=0.7, m_probability_else=0.1
)

# The record to be matched
record = {
    "unique_id": 1,
    "first_name": "Eliza",
    "surname": "Smith",
    "dob": "1971-05-24",
    "city": "London",
    "email": "eliza@smith.net",
    "group": 10000,
}


def test_tf_tables_init_works():
    for s in [settings_tf, settings_no_tf, settings]:
        linker = DuckDBLinker(
            df,
            s,
        )

        # Compute tf table for first name
        # This:
        # 1. Does nothing if term frequencies are not used
        # 2. Should use the cache and not break if tf adj is requested for fn
        # 3. Use both the cache and also create surname in our final example
        linker.compute_tf_table("first_name")

        # Running without _df_concat_with_tf
        linker.__deepcopy__(None).find_matches_to_new_records(
            [record], blocking_rules=[], match_weight_threshold=-10000
        )

        # Trial for if _df_concat_with_tf already exists...
        linker._initialise_df_concat_with_tf(materialise=True)
        linker.find_matches_to_new_records(
            [record], blocking_rules=[], match_weight_threshold=-10000
        )


def test_matches_work():
    linker = DuckDBLinker(
        df,
        settings,
    )

    # Train our model to get more reasonable outputs...
    linker.estimate_u_using_random_sampling(max_pairs=1e6)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    brs = ["l.surname = r.surname"]

    matches = linker.find_matches_to_new_records(
        [record], blocking_rules=brs, match_weight_threshold=-10000
    )

    matches = matches.as_pandas_dataframe()
    assert len(matches) == 10

    matches = linker.find_matches_to_new_records(
        [record], blocking_rules=brs, match_weight_threshold=0
    )

    matches = matches.as_pandas_dataframe()
    assert len(matches) == 2
