from datetime import datetime

import pytest

from splink.comparison_level_library import (
    AbsoluteDateDifferenceLevel,
    AbsoluteTimeDifferenceLevel,
)
from splink.comparison_library import AbsoluteDateDifferenceAtThresholds
from tests.decorator import mark_with_dialects_excluding, mark_with_dialects_including
from tests.literal_utils import run_comparison_vector_value_tests, run_is_in_level_tests


@mark_with_dialects_excluding("sqlite")
def test_absolute_date_difference_level(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "level": AbsoluteDateDifferenceLevel(
                metric="day",
                col_name="dob",
                input_is_string=False,
                threshold=29,
            ),
            "inputs": [
                {
                    "dob_l": datetime(2000, 1, 1),
                    "dob_r": datetime(2000, 1, 28),
                    "expected": True,
                },
                {
                    "dob_l": datetime(2000, 1, 1),
                    "dob_r": datetime(2000, 1, 31),
                    "expected": False,
                },
            ],
        },
        {
            "level": AbsoluteDateDifferenceLevel(
                metric="day",
                col_name="dob",
                input_is_string=True,
                threshold=29,
            ),
            "inputs": [
                {
                    "dob_l": "2000-01-01",
                    "dob_r": "2000-01-28",
                    "expected": True,
                },
                {
                    "dob_l": "2000-01-01",
                    "dob_r": "2000-01-31",
                    "expected": False,
                },
            ],
        },
    ]

    run_is_in_level_tests(test_cases, db_api)


@mark_with_dialects_excluding("sqlite")
def test_absolute_time_difference_levels(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "level": AbsoluteTimeDifferenceLevel(
                metric="minute",
                col_name="time",
                input_is_string=True,
                threshold=1,
            ),
            "inputs": [
                {
                    "time_l": "2023-02-07T14:45:00Z",
                    "time_r": "2023-02-07T14:45:59Z",
                    "expected": True,
                },
                {
                    "time_l": "2023-02-07T14:45:00Z",
                    "time_r": "2023-02-07T14:46:01Z",
                    "expected": False,
                },
            ],
        },
        {
            "level": AbsoluteTimeDifferenceLevel(
                metric="second",
                col_name="time",
                input_is_string=False,
                threshold=61,
            ),
            "inputs": [
                {
                    "time_l": datetime(2023, 2, 7, 14, 45, 0),
                    "time_r": datetime(2023, 2, 7, 14, 46, 0),
                    "expected": True,
                },
                {
                    "time_l": datetime(2023, 2, 7, 14, 45, 0),
                    "time_r": datetime(2023, 2, 7, 14, 46, 2),
                    "expected": False,
                },
            ],
        },
    ]

    run_is_in_level_tests(test_cases, db_api)


@mark_with_dialects_excluding("sqlite")
def test_absolute_date_difference_at_thresholds(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": AbsoluteDateDifferenceAtThresholds(
                "dob",
                thresholds=[1, 2],
                metrics=["day", "month"],
                input_is_string=True,
            ),
            "inputs": [
                {
                    "dob_l": "2000-01-01",
                    "dob_r": "2020-01-01",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
                {
                    "dob_l": "2000-01-01",
                    "dob_r": "2000-01-15",
                    "expected_value": 1,
                    "expected_label": "Abs difference of 'transformed dob <= 2 month'",
                },
                {
                    "dob_l": "2000-ab-cd",
                    "dob_r": "2000-01-28",
                    "expected_value": -1,
                    "expected_label": "transformed dob is NULL",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_alternative_date_format(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": AbsoluteDateDifferenceAtThresholds(
                "dob",
                thresholds=[3, 2],
                metrics=["day", "month"],
                input_is_string=True,
                datetime_format="%Y/%m/%d",
            ),
            "inputs": [
                {
                    "dob_l": "2000/01/01",
                    "dob_r": "2020/01/01",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
                {
                    "dob_l": "2000/01/01",
                    "dob_r": "2000/01/15",
                    "expected_value": 1,
                    "expected_label": "Abs difference of 'transformed dob <= 2 month'",
                },
                {
                    "dob_l": "2000/01/01",
                    "dob_r": "2000/01/02",
                    "expected_value": 2,
                    "expected_label": "Abs difference of 'transformed dob <= 3 day'",
                },
                {
                    "dob_l": "2000/ab/cd",
                    "dob_r": "2000/01/28",
                    "expected_value": -1,
                    "expected_label": "transformed dob is NULL",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


@mark_with_dialects_excluding("sqlite")
def test_time_difference_error_logger(dialect):
    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        AbsoluteDateDifferenceAtThresholds(
            "dob",
            thresholds=[1],
            metrics=["day", "month", "year", "year"],
            input_is_string=True,
        )
    # Negative threshold
    with pytest.raises(ValueError):
        AbsoluteDateDifferenceAtThresholds(
            "dob", thresholds=[-1], metrics=["day"], input_is_string=True
        )
    # Invalid metric
    with pytest.raises(ValueError):
        AbsoluteDateDifferenceAtThresholds(
            "dob", thresholds=[1], metrics=["dy"], input_is_string=True
        )
    # Threshold len == 0
    with pytest.raises(ValueError):
        AbsoluteDateDifferenceAtThresholds(
            "dob", thresholds=[], metrics=["dy"], input_is_string=True
        )
    # Metric len == 0
    with pytest.raises(ValueError):
        AbsoluteDateDifferenceAtThresholds(
            "dob", thresholds=[1], metrics=[], input_is_string=True
        )
