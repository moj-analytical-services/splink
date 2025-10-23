import gc

import pandas as pd
import pytest

import splink.internals.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink.internals.column_expression import ColumnExpression
from splink.internals.comparison_creator import ComparisonCreator
from splink.internals.comparison_level_creator import ComparisonLevelCreator

from .decorator import mark_with_dialects_excluding

comparison_first_name = {
    "output_column_name": "first_name",
    "comparison_levels": [
        cll.NullLevel("first_name"),
        cll.ExactMatchLevel("first_name", term_frequency_adjustments=True),
        cll.LevenshteinLevel("first_name", 2).configure(m_probability=0.2),
        cll.ElseLevel(),
    ],
}
comparison_surname = {
    "output_column_name": "surname",
    "comparison_levels": [
        cll.NullLevel("surname"),
        cll.ExactMatchLevel("surname", term_frequency_adjustments=True),
        cll.LevenshteinLevel("surname", 2).configure(
            label_for_charts="surname Levenshtein under 2"
        ),
        cll.ElseLevel().configure(m_probability=0.2, u_probability=0.85),
    ],
}
comparison_city = {
    "output_column_name": "city",
    "comparison_levels": [
        cll.NullLevel("city"),
        cll.ExactMatchLevel("city", term_frequency_adjustments=True),
        cll.LevenshteinLevel("city", 1),
        cll.LevenshteinLevel("city", 2),
        cll.ElseLevel(),
    ],
}
comparison_email = {
    "output_column_name": "dob",
    "comparison_levels": [
        cll.NullLevel("dob"),
        cll.ExactMatchLevel("dob"),
        cll.CustomLevel("substr(dob_l, 1, 4) = substr(dob_r, 1, 4)", "year matches"),
        {
            "sql_condition": "substr(dob_l, 1, 2) = substr(dob_r, 1, 2)",
            "label_for_charts": "century matches",
        },
        cll.LevenshteinLevel("dob", 3),
        cll.ElseLevel(),
    ],
}

cll_settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        comparison_first_name,
        comparison_surname,
        comparison_city,
        comparison_email,
    ],
    "blocking_rules_to_generate_predictions": [
        "l.dob = r.dob",
        "l.first_name = r.first_name",
    ],
}


@mark_with_dialects_excluding()
def test_cll_creators_run_predict(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = helper.Linker(df, cll_settings, **helper.extra_linker_args())
    linker.inference.predict()


@mark_with_dialects_excluding()
def test_cll_creators_instantiate_levels(dialect):
    cll.NullLevel("city").get_comparison_level(dialect)
    cll.ElseLevel().get_comparison_level(dialect)
    cll.ExactMatchLevel("city").get_comparison_level(dialect)
    cll.LevenshteinLevel("city", 5).get_comparison_level(dialect)


@mark_with_dialects_excluding()
def test_cll_creators_instantiate_levels_with_config(dialect):
    lev_dict = cll.NullLevel("city").get_comparison_level(dialect).as_dict()
    assert lev_dict["is_null_level"]

    lev_dict = (
        cll.ElseLevel()
        .configure(m_probability=0.2, u_probability=0.7)
        .get_comparison_level(dialect)
        .as_dict()
    )
    assert lev_dict["m_probability"] == 0.2
    assert lev_dict["u_probability"] == 0.7

    lev_dict = (
        cll.ExactMatchLevel("city")
        .configure(tf_adjustment_column="city", tf_adjustment_weight=0.9)
        .get_comparison_level(dialect)
        .as_dict()
    )
    assert lev_dict["tf_adjustment_column"] == "city"
    assert lev_dict["tf_adjustment_weight"] == 0.9

    lev_dict = (
        cll.LevenshteinLevel("city", 5)
        .configure(label_for_charts="city loose fuzzy match")
        .get_comparison_level(dialect)
        .as_dict()
    )
    assert lev_dict["label_for_charts"] == "city loose fuzzy match"


comparison_name = cl.CustomComparison(
    output_column_name="name",
    comparison_levels=[
        cll.CustomLevel(
            "(first_name_l IS NULL OR first_name_r IS NULL) AND "
            "(surname_l IS NULL OR surname_r IS NULL) "
        ).configure(is_null_level=True),
        {
            "sql_condition": ("first_name_l || surname_l = first_name_r || surname_r"),
            "label_for_charts": "both names matching",
        },
        cll.CustomLevel(
            (
                "levenshtein("
                "first_name_l || surname_l, "
                "first_name_r || surname_r"
                ") <= 3"
            ),
            "both names fuzzy matching",
        ),
        cll.ExactMatchLevel("first_name"),
        cll.ExactMatchLevel("surname"),
        cll.ElseLevel(),
    ],
)
comparison_city = cl.ExactMatch("city").configure(u_probabilities=[0.6, 0.4])
comparison_email = cl.LevenshteinAtThresholds("email", 3).configure(
    m_probabilities=[0.8, 0.1, 0.1]
)
comparison_dob = cl.LevenshteinAtThresholds("dob", [1, 2])

cl_settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        comparison_name,
        comparison_city,
        comparison_email,
        comparison_dob,
    ],
    "blocking_rules_to_generate_predictions": [
        "l.dob = r.dob",
        "l.first_name = r.first_name",
    ],
}


@mark_with_dialects_excluding()
def test_cl_creators_run_predict(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = helper.Linker(df, cl_settings, **helper.extra_linker_args())

    linker.inference.predict()


@mark_with_dialects_excluding("sqlite")
def test_regex_fall_through(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = pd.DataFrame(
        [
            {"unique_id": 1, "name": "groat"},
            {"unique_id": 2, "name": "float"},
        ]
    )
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            {
                "comparison_levels": [
                    cll.NullLevel("name"),
                    # this pattern does not match any data:
                    cll.ExactMatchLevel(
                        ColumnExpression("name").regex_extract("^wr.*")
                    ),
                    cll.ElseLevel(),
                ]
            }
        ],
    }

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.inference.predict().as_pandas_dataframe()

    # only entry should be in Else level
    assert df_e["gamma_name"][0] == 0


@mark_with_dialects_excluding("sqlite")
def test_null_pattern_match(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = pd.DataFrame(
        [
            {"unique_id": 1, "name": "groat"},
            {"unique_id": 2, "name": "float"},
        ]
    )
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            {
                "comparison_levels": [
                    # this pattern does matches no data:
                    cll.NullLevel("name", valid_string_pattern=".*ook"),
                    cll.ExactMatchLevel(ColumnExpression("name")),
                    cll.ElseLevel(),
                ]
            }
        ],
    }

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.inference.predict().as_pandas_dataframe()

    # only entry should be in Null level
    assert df_e["gamma_name"][0] == -1


comparison_email_cl = cl.EmailComparison(
    "email",
)
comparison_name_cl = cl.NameComparison(
    "first_name",
)

comparison_dob_cl = cl.DateOfBirthComparison(
    ColumnExpression("dob"),
    input_is_string=True,
)
comparison_forenamesurname_cl = cl.ForenameSurnameComparison(
    "first_name",
    "surname",
)

cl_settings_2 = {
    "link_type": "dedupe_only",
    "comparisons": [
        comparison_name_cl,
        # obviously not realistic:
        comparison_forenamesurname_cl,
        comparison_email_cl,
        comparison_dob_cl,
    ],
    "blocking_rules_to_generate_predictions": [
        "l.dob = r.dob",
        "l.first_name = r.first_name",
    ],
}


@mark_with_dialects_excluding("sqlite", "postgres")
def test_ctl_creators_run_predict(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = helper.Linker(df, cl_settings_2, **helper.extra_linker_args())
    linker.inference.predict()


def test_custom_dialect_no_string_lookup():
    from splink.internals.dialects import SplinkDialect

    # force garbage collection so we forget about any other test dialects
    # previously defined
    gc.collect()

    class TestDialectNoLookup(SplinkDialect):
        # missing _dialect_name_for_factory!
        @property
        def sql_dialect_str(self):
            return "test_dialect"

        @property
        def sqlglot_dialect(self):
            return "duckdb"

    # the existence of TestDialectNoLookup should not impact our ability
    # to use other dialects
    cll.ExactMatchLevel("some_column").get_comparison_level("duckdb")


def test_custom_dialect_duplicate_string_lookup():
    from splink.internals.dialects import SplinkDialect

    # force garbage collection so we forget about any other test dialects
    # previously defined
    gc.collect()

    class TestDialectDuplicateFactoryName(SplinkDialect):
        # we already have a duckdb dialect!
        _dialect_name_for_factory = "duckdb"

        @property
        def sql_dialect_str(self):
            return "test_dialect"

        @property
        def sqlglot_dialect(self):
            return "duckdb"

    # should get an error as level doesn't know which 'duckdb' we mean
    with pytest.raises(ValueError) as exc_info:
        cll.ExactMatchLevel("some_column").get_comparison_level("duckdb")
    assert "Found multiple subclasses" in str(exc_info.value)

    # should be able to use spark still
    cll.ExactMatchLevel("some_column").get_comparison_level("spark")


def test_valid_custom_dialect():
    from splink.internals.dialects import SplinkDialect

    # force garbage collection so we forget about any other test dialects
    # previously defined
    gc.collect()

    class TestDialect(SplinkDialect):
        _dialect_name_for_factory = "valid_test_dialect"

        @property
        def sql_dialect_str(self):
            return "test_dialect"

        @property
        def sqlglot_dialect(self):
            return "duckdb"

        # helper for tests that allows SplinkDialect to forget about this dialect
        # don't need to do this previously as they don't get instantiated
        def _delete_instance(self):
            del super()._dialect_instances[type(self)]

    cll.ExactMatchLevel("some_column").get_comparison_level("valid_test_dialect")
    TestDialect()._delete_instance()


def test_invalid_dialect():
    # force garbage collection so we forget about any other test dialects
    # previously defined
    gc.collect()

    # no such dialect defined!
    with pytest.raises(ValueError) as exc_info:
        cll.ExactMatchLevel("some_column").get_comparison_level("bad_test_dialect")
    assert "Could not find subclass" in str(exc_info.value)


def test_cl_configure():
    # this is fine:
    cl.LevenshteinAtThresholds("col", [1, 2, 3]).configure(
        m_probabilities=[0.4, 0.1, 0.1, 0.3, 0.1]
    )

    with pytest.raises(ValueError):
        # too many probabilities
        cl.ExactMatch("col").configure(m_probabilities=[0.5, 0.3, 0.2])

    with pytest.raises(ValueError):
        # too few probabilities
        cl.LevenshteinAtThresholds("col", [1, 2]).configure(u_probabilities=[0.5, 0.5])


def test_comparison_reconfigure():
    def assert_tf_adjustments_on_off(
        comparison: ComparisonCreator, tf_adjustments_on: bool = False
    ) -> None:
        levels = comparison.get_configured_comparison_levels()
        # the comparisons in this test should have null, then exact:
        exact_match_comparison_level = levels[1]
        # check we _really_ have an exact match level
        assert exact_match_comparison_level.is_exact_match_level

        cl_dict = exact_match_comparison_level.create_level_dict("duckdb")
        if tf_adjustments_on:
            assert "tf_adjustment_column" in cl_dict
        else:
            assert "tf_adjustment_column" not in cl_dict

    plain_comparison = cl.LevenshteinAtThresholds("col")
    assert_tf_adjustments_on_off(plain_comparison, tf_adjustments_on=False)

    comp_with_tf = plain_comparison.configure(term_frequency_adjustments=True)
    assert_tf_adjustments_on_off(comp_with_tf, tf_adjustments_on=True)

    # turn tf adjustments off again
    comp_without_tf = comp_with_tf.configure(term_frequency_adjustments=False)
    assert_tf_adjustments_on_off(comp_without_tf, tf_adjustments_on=False)


def test_comparison_level_reconfigure():
    def assert_tf_adjustments_on_off(
        comparison_level: ComparisonLevelCreator, tf_adjustments_on: bool = False
    ) -> None:
        cl_dict = comparison_level.create_level_dict("duckdb")
        if tf_adjustments_on:
            assert "tf_adjustment_column" in cl_dict
        else:
            assert "tf_adjustment_column" not in cl_dict

    exact_match_comparison_level = cll.ExactMatchLevel("col")
    assert_tf_adjustments_on_off(exact_match_comparison_level, tf_adjustments_on=False)

    exact_match_with_tf = exact_match_comparison_level.configure(
        tf_adjustment_column="col"
    )
    assert_tf_adjustments_on_off(exact_match_with_tf, tf_adjustments_on=True)

    # turn tf adjustments off again
    exact_match_without_tf = exact_match_with_tf.configure(tf_adjustment_column=None)
    assert_tf_adjustments_on_off(exact_match_without_tf, tf_adjustments_on=False)


def test_sequential_configurations():
    # want to check that configurations don't forget about previously-set options
    em_with_m = cl.ExactMatch("col").configure(m_probabilities=[0.8, 0.2])
    assert em_with_m.m_probabilities == [0.8, 0.2]
    assert not em_with_m.term_frequency_adjustments

    em_with_m.configure(term_frequency_adjustments=True)
    assert em_with_m.term_frequency_adjustments
    assert em_with_m.m_probabilities == [0.8, 0.2]

    em_with_m.configure(m_probabilities=[0.9, 0.1])
    assert em_with_m.m_probabilities == [0.9, 0.1]
    assert em_with_m.term_frequency_adjustments
