"""Regression tests for issue #3344.

A comparison level configured with ``tf_adjustment_weight`` of 0.0 used to come
back as 1.0 after a dict round trip, because ``ComparisonLevel.as_dict()`` omitted
exactly-zero weights while the constructor default is 1.0.
"""

import pandas as pd

import splink.internals.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink.internals.comparison_level import ComparisonLevel
from splink.internals.dialects import SplinkDialect
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker


def test_as_dict_round_trip_preserves_zero_weight():
    level = (
        cll.ExactMatchLevel("first_name")
        .configure(tf_adjustment_column="first_name", tf_adjustment_weight=0.0)
        .get_comparison_level("duckdb")
    )

    level_dict = level.as_dict()
    assert level_dict["tf_adjustment_weight"] == 0.0

    rebuilt = ComparisonLevel(
        **level_dict, sql_dialect=SplinkDialect.from_string("duckdb")
    )
    assert rebuilt._tf_adjustment_weight == 0.0
    assert rebuilt.as_dict()["tf_adjustment_weight"] == 0.0


def test_comparison_creator_path_preserves_zero_weight():
    comparison = cl.CustomComparison(
        output_column_name="first_name",
        comparison_levels=[
            cll.NullLevel("first_name"),
            cll.ExactMatchLevel("first_name").configure(
                tf_adjustment_column="first_name", tf_adjustment_weight=0.0
            ),
            cll.ElseLevel(),
        ],
    ).get_comparison("duckdb")

    levels = comparison.as_dict()["comparison_levels"]
    assert levels[1]["tf_adjustment_weight"] == 0.0


def make_settings(with_tf_column):
    exact_match_level = {
        "sql_condition": '"first_name_l" = "first_name_r"',
        "label_for_charts": "exact",
        "m_probability": 0.9,
        "u_probability": 0.01,
    }
    if with_tf_column:
        exact_match_level["tf_adjustment_column"] = "first_name"
        exact_match_level["tf_adjustment_weight"] = 0.0

    return {
        "link_type": "dedupe_only",
        "probability_two_random_records_match": 0.01,
        "retain_intermediate_calculation_columns": True,
        "blocking_rules_to_generate_predictions": ["l.surname = r.surname"],
        "comparisons": [
            {
                "output_column_name": "first_name",
                "comparison_levels": [
                    {
                        "sql_condition": (
                            '"first_name_l" IS NULL OR "first_name_r" IS NULL'
                        ),
                        "label_for_charts": "null",
                        "is_null_level": True,
                    },
                    exact_match_level,
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "else",
                        "m_probability": 0.1,
                        "u_probability": 0.99,
                    },
                ],
            }
        ],
    }


def test_predict_zero_weight_equals_level_without_tf_column():
    df = pd.DataFrame(
        {
            "unique_id": [1, 2, 3, 4, 5, 6],
            "first_name": ["alice", "alice", "alice", "bob", "bob", "carol"],
            "surname": ["smith"] * 6,
        }
    )

    linker_tf = Linker(
        DuckDBAPI(connection=":memory:").register(df), make_settings(True)
    )
    df_predict_tf = linker_tf.inference.predict().as_pandas_dataframe()
    exact_tf = df_predict_tf.query("gamma_first_name == 1")

    # a weight of 0.0 means the TF adjustment itself must be exactly zero
    assert (exact_tf["mw_tf_adj_first_name"] == 0.0).all()

    # ... and the level must score the same as one without a TF column at all
    linker_no_tf = Linker(
        DuckDBAPI(connection=":memory:").register(df), make_settings(False)
    )
    df_predict_no_tf = linker_no_tf.inference.predict().as_pandas_dataframe()
    exact_no_tf = df_predict_no_tf.query("gamma_first_name == 1")

    assert sorted(exact_tf["mw_first_name"].round(10)) == sorted(
        exact_no_tf["mw_first_name"].round(10)
    )
