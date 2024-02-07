import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink.column_expression import ColumnExpression
from tests.decorator import mark_with_dialects_excluding
from tests.literal_utils import (
    ComparisonLevelTestSpec,
    ComparisonTestSpec,
    LiteralTestValues,
    run_tests_with_args,
)


@mark_with_dialects_excluding("sqlite")
def test_absolute_time_difference_levels_date(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    col_exp = ColumnExpression("dob").try_parse_date()
    test_spec = ComparisonLevelTestSpec(
        cll.AbsoluteTimeDifferenceLevel,
        default_keyword_args={
            "date_metric": "day",
            "col_name": col_exp,
        },
        tests=[
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2000-01-28"},
                keyword_arg_overrides={"date_threshold": 30},
                expected_in_level=True,
            ),
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2000-01-28"},
                keyword_arg_overrides={"date_threshold": 26},
                expected_in_level=False,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("sqlite")
def test_absolute_time_difference_levels_timestamp(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    col_exp = ColumnExpression("dob").try_parse_timestamp()
    test_spec = ComparisonLevelTestSpec(
        cll.AbsoluteTimeDifferenceLevel,
        default_keyword_args={
            "date_metric": "second",
            "col_name": col_exp,
        },
        tests=[
            LiteralTestValues(
                {"dob_l": "2023-02-07T14:45:00Z", "dob_r": "2023-02-07T14:46:00Z"},
                keyword_arg_overrides={"date_threshold": 61},
                expected_in_level=True,
            ),
            LiteralTestValues(
                {"dob_l": "2023-02-07T14:45:00Z", "dob_r": "2023-02-07T14:46:00Z"},
                keyword_arg_overrides={"date_threshold": 59},
                expected_in_level=False,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("sqlite")
def test_absolute_time_difference_at_thresholds(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    test_spec = ComparisonTestSpec(
        cl.AbsoluteTimeDifferenceAtThresholds(
            "dob",
            date_thresholds=[1],
            date_metrics=["day"],
            cast_strings_to_datetimes=True,
        ),
        tests=[
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2000-01-28"},
                expected_gamma_val=0,
            )
        ],
    )

    run_tests_with_args(test_spec, db_api)
