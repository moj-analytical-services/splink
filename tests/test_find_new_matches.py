from copy import deepcopy

import pandas as pd

import splink.internals.comparison_library as cl
from splink.internals.blocking_rule_library import block_on
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import compute_df_concat_with_tf

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")


def get_different_settings_dicts():
    settings = get_settings_dict()
    settings_tf = deepcopy(settings, None)
    # Settings with two term frequency columns
    settings_tf["comparisons"][1] = cl.ExactMatch("surname").configure(
        term_frequency_adjustments=True,
        m_probabilities=[0.7, 0.1],
    )
    settings_no_tf = deepcopy(settings, None)
    # Settings with no term frequencies
    settings_no_tf["comparisons"][0] = cl.ExactMatch("first_name").configure(
        term_frequency_adjustments=False,
        m_probabilities=[0.7, 0.1],
    )
    return settings_tf, settings_no_tf, settings


# The record to be matched
record = {
    "unique_id": 1,
    "first_name": "Eliza",
    "surname": "Smith",
    "dob": "1971-05-24",
    "city": "London",
    "email": "eliza@smith.net",
    "cluster": 10000,
}


@mark_with_dialects_excluding()
def test_tf_tables_init_works(test_helpers, dialect):
    helper = test_helpers[dialect]
    Linker = helper.Linker

    for idx, s in enumerate(get_different_settings_dicts()):
        linker = Linker(
            df,
            s,
            **helper.extra_linker_args(),
            input_table_aliases=f"test_tf_table_alias_{idx}",
        )

        # Compute tf table for first name
        # This:
        # 1. Does nothing if term frequencies are not used
        # 2. Should use the cache and not break if tf adj is requested for fn
        # 3. Use both the cache and also create surname in our final example
        linker.table_management.compute_tf_table("first_name")

        # Running without _df_concat_with_tf
        linker.inference.find_matches_to_new_records(
            [record],
            blocking_rules=[],
            match_weight_threshold=-10000,
        )

        # Trial for if _df_concat_with_tf already exists...
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)

        linker.inference.find_matches_to_new_records(
            [record], blocking_rules=[], match_weight_threshold=-10000
        )


@mark_with_dialects_excluding()
def test_matches_work(test_helpers, dialect):
    helper = test_helpers[dialect]
    Linker = helper.Linker

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = Linker(df, get_settings_dict(), **helper.extra_linker_args())

    # Train our model to get more reasonable outputs...
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.visualisations.match_weights_chart().save("mwc.html")

    blocking_rule = block_on("first_name", "surname")
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)
    # linker.visualisations.match_weights_chart().save("mwc.html")

    brs = ["l.surname = r.surname"]

    matches = linker.inference.find_matches_to_new_records(
        [record], blocking_rules=brs, match_weight_threshold=-10000
    )

    matches = matches.as_pandas_dataframe()
    assert len(matches) == 10

    # linker.visualisations.match_weights_chart().save("mwc.html")

    matches = linker.inference.find_matches_to_new_records(
        [record], blocking_rules=brs, match_weight_threshold=0.1
    )

    matches = matches.as_pandas_dataframe()
    assert len(matches) == 2
