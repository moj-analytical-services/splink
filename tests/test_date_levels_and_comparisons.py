from datetime import datetime

import pytest

import splink.comparison_level_library as cll
import splink.comparison_library as cl
import splink.comparison_template_library as ctl
from splink.column_expression import ColumnExpression
from tests.decorator import mark_with_dialects_excluding, mark_with_dialects_including
from tests.literal_utils import (
    ComparisonLevelTestSpec,
    ComparisonTestSpec,
    LiteralTestValues,
    run_tests_with_args,
)


@mark_with_dialects_excluding("sqlite")
def test_absolute_date_difference_level(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    col_exp = ColumnExpression("dob").try_parse_date()
    test_spec = ComparisonLevelTestSpec(
        cll.AbsoluteDateDifferenceLevel,
        default_keyword_args={
            "metric": "day",
            "col_name": col_exp,
            "input_is_string": False,
        },
        tests=[
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2000-01-28"},
                keyword_arg_overrides={"threshold": 30},
                expected_in_level=True,
            ),
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2000-01-28"},
                keyword_arg_overrides={"threshold": 26},
                expected_in_level=False,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonLevelTestSpec(
        cll.AbsoluteDateDifferenceLevel,
        default_keyword_args={
            "metric": "day",
            "col_name": "dob",
            "input_is_string": True,
        },
        tests=[
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2000-01-28"},
                keyword_arg_overrides={"threshold": 30},
                expected_in_level=True,
            ),
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2000-01-28"},
                keyword_arg_overrides={"threshold": 26},
                expected_in_level=False,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("sqlite")
def test_absolute_time_difference_levels(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    test_spec = ComparisonLevelTestSpec(
        cll.AbsoluteTimeDifferenceLevel,
        default_keyword_args={
            "metric": "minute",
            "col_name": "time",
            "input_is_string": True,
            "threshold": 1,
        },
        tests=[
            LiteralTestValues(
                values={
                    "time_l": "2023-02-07T14:45:00Z",
                    "time_r": "2023-02-07T14:45:59Z",
                },
                expected_in_level=True,
            ),
            LiteralTestValues(
                values={
                    "time_l": "2023-02-07T14:45:00Z",
                    "time_r": "2023-02-07T14:46:01Z",
                },
                expected_in_level=False,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonLevelTestSpec(
        cll.AbsoluteTimeDifferenceLevel,
        default_keyword_args={
            "metric": "second",
            "col_name": "time",
            "input_is_string": False,
        },
        tests=[
            LiteralTestValues(
                {
                    "time_l": datetime(2023, 2, 7, 14, 45, 0),
                    "time_r": datetime(2023, 2, 7, 14, 46, 0),
                },
                keyword_arg_overrides={"threshold": 61},
                expected_in_level=True,
            ),
            LiteralTestValues(
                {
                    "time_l": datetime(2023, 2, 7, 14, 45, 0),
                    "time_r": datetime(2023, 2, 7, 14, 46, 0),
                },
                keyword_arg_overrides={"threshold": 59},
                expected_in_level=False,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("sqlite")
def test_absolute_date_difference_at_thresholds(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    test_spec = ComparisonTestSpec(
        cl.AbsoluteDateDifferenceAtThresholds(
            "dob",
            thresholds=[1, 2],
            metrics=["day", "month"],
            input_is_string=True,
        ),
        tests=[
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2020-01-01"},
                expected_gamma_val=0,
            ),
            LiteralTestValues(
                values={"dob_l": "2000-01-01", "dob_r": "2000-01-15"},
                expected_gamma_val=1,
            ),
            LiteralTestValues(
                values={"dob_l": "2000-ab-cd", "dob_r": "2000-01-28"},
                expected_gamma_val=-1,
            ),
        ],
    )

    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_alternative_date_format(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    test_spec = ComparisonTestSpec(
        cl.AbsoluteDateDifferenceAtThresholds(
            "dob",
            thresholds=[3, 2],
            metrics=["day", "month"],
            input_is_string=True,
            datetime_format="%Y/%m/%d",
        ),
        tests=[
            LiteralTestValues(
                values={"dob_l": "2000/01/01", "dob_r": "2020/01/01"},
                expected_gamma_val=0,
            ),
            LiteralTestValues(
                values={"dob_l": "2000/01/01", "dob_r": "2000/01/15"},
                expected_gamma_val=1,
            ),
            LiteralTestValues(
                values={"dob_l": "2000/01/01", "dob_r": "2000/01/02"},
                expected_gamma_val=2,
            ),
            LiteralTestValues(
                values={"dob_l": "2000/ab/cd", "dob_r": "2000/01/28"},
                expected_gamma_val=-1,
            ),
        ],
    )

    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("sqlite")
def test_time_difference_error_logger(dialect):
    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        cl.AbsoluteDateDifferenceAtThresholds(
            "dob",
            thresholds=[1],
            metrics=["day", "month", "year", "year"],
            input_is_string=True,
        )
    # Negative threshold
    with pytest.raises(ValueError):
        cl.AbsoluteDateDifferenceAtThresholds(
            "dob", thresholds=[-1], metrics=["day"], input_is_string=True
        )
    # Invalid metric
    with pytest.raises(ValueError):
        cl.AbsoluteDateDifferenceAtThresholds(
            "dob", thresholds=[1], metrics=["dy"], input_is_string=True
        )
    # Threshold len == 0
    with pytest.raises(ValueError):
        cl.AbsoluteDateDifferenceAtThresholds(
            "dob", thresholds=[], metrics=["dy"], input_is_string=True
        )
    # Metric len == 0
    with pytest.raises(ValueError):
        cl.AbsoluteDateDifferenceAtThresholds(
            "dob", thresholds=[1], metrics=[], input_is_string=True
        )


@mark_with_dialects_excluding("sqlite")
def test_date_comparison(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    # Note that the input here is of type date not string
    # because we pass ColumnExpression("dob").try_parse_date()
    test_spec = ComparisonTestSpec(
        ctl.DateComparison(
            ColumnExpression("dob").try_parse_date(),
            datetime_metrics=["day", "month"],
            datetime_thresholds=[4, 2],
            separate_1st_january=True,
        ),
        tests=[
            LiteralTestValues(
                values={
                    "dob_l": "2020-01-01",
                    "dob_r": "2020-01-01",
                },
                expected_gamma_val=5,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2023-02-07",
                    "dob_r": "2023-02-07",
                },
                expected_gamma_val=4,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2123-02-07",
                    "dob_r": "2023-02-07",
                },
                expected_gamma_val=3,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2023-02-09",
                    "dob_r": "2023-02-11",
                },
                expected_gamma_val=2,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2023-03-06",
                    "dob_r": "2023-02-07",
                },
                expected_gamma_val=1,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2023-03-06",
                    "dob_r": "2023-09-12",
                },
                expected_gamma_val=0,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    # These tests differ in that input is string
    test_spec = ComparisonTestSpec(
        ctl.DateComparison(
            ColumnExpression("dob"),
            datetime_metrics=["day", "month"],
            datetime_thresholds=[4, 2],
            input_is_string=True,
            separate_1st_january=True,
        ),
        tests=[
            LiteralTestValues(
                values={
                    "dob_l": "2020-01-01",
                    "dob_r": "2020-01-01",
                },
                expected_gamma_val=5,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2023-02-07",
                    "dob_r": "2023-02-07",
                },
                expected_gamma_val=4,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2123-02-07",
                    "dob_r": "2023-02-07",
                },
                expected_gamma_val=3,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2023-02-09",
                    "dob_r": "2023-02-11",
                },
                expected_gamma_val=2,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2023-03-06",
                    "dob_r": "2023-02-07",
                },
                expected_gamma_val=1,
            ),
            LiteralTestValues(
                values={
                    "dob_l": "2023-03-06",
                    "dob_r": "2023-09-12",
                },
                expected_gamma_val=0,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_date_comparison_error_logger(dialect):
    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        ctl.DateComparison(
            "date", datetime_thresholds=[1, 2], datetime_metrics=["month"]
        ).get_comparison(dialect)
    # Check metric and threshold are the correct way around
    with pytest.raises(TypeError):
        ctl.DateComparison(
            "date", datetime_thresholds=["month"], datetime_metrics=[1]
        ).get_comparison(dialect)
    # Invalid metric
    with pytest.raises(ValueError):
        ctl.DateComparison(
            "date", datetime_thresholds=[1], datetime_metrics=["dy"]
        ).get_comparison(dialect)
    # Threshold len == 0
    with pytest.raises(ValueError):
        ctl.DateComparison(
            "date", datetime_thresholds=[], datetime_metrics=["day"]
        ).get_comparison(dialect)
    # Metric len == 0
    with pytest.raises(ValueError):
        ctl.DateComparison(
            "date", datetime_thresholds=[1], datetime_metrics=[]
        ).get_comparison(dialect)
