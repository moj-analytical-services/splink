from datetime import datetime

from splink import DuckDBAPI
from splink.comparison_level_library import (
    AbsoluteDateDifferenceLevel,
    ArrayIntersectLevel,
    ElseLevel,
    ExactMatchLevel,
    NullLevel,
)
from splink.comparison_library import ArrayIntersectAtSizes, ExactMatch
from tests.literal_utils import run_comparison_vector_value_tests, run_is_in_level_tests


def test_is_in_level():
    db_api = DuckDBAPI()

    test_cases = [
        {
            "level": ExactMatchLevel("name"),
            "inputs": [
                {"name_l": "John", "name_r": "John", "expected": True},
                {"name_l": "John", "name_r": "Jane", "expected": False},
            ],
        },
        {
            "level": NullLevel("name"),
            "inputs": [
                {"name_l": None, "name_r": "John", "expected": True},
                {"name_l": "John", "name_r": None, "expected": True},
                {"name_l": "John", "name_r": "Jane", "expected": False},
            ],
        },
        {
            "level": AbsoluteDateDifferenceLevel(
                "date", input_is_string=False, threshold=3, metric="day"
            ),
            "inputs": [
                {
                    "date_l": datetime(2023, 1, 1),
                    "date_r": datetime(2023, 1, 3),
                    "expected": True,
                },
                {
                    "date_l": datetime(2023, 1, 1),
                    "date_r": datetime(2023, 1, 5),
                    "expected": False,
                },
            ],
        },
        {
            "level": ArrayIntersectLevel("tags", 2),
            "inputs": [
                {"tags_l": [1, 2, 3], "tags_r": [2, 3, 4], "expected": True},
                {"tags_l": [1, 2, 3], "tags_r": [4, 5, 6], "expected": False},
            ],
        },
        {
            "level": ElseLevel(),
            "inputs": [
                {"name_l": "John", "name_r": "Jane", "expected": True},
            ],
        },
    ]

    run_is_in_level_tests(test_cases, db_api)


def test_comparison_vector_value():
    db_api = DuckDBAPI()
    test_cases = [
        {
            "comparison": ExactMatch("name"),
            "inputs": [
                {
                    "name_l": "John",
                    "name_r": "John",
                    "expected_value": 1,
                    "expected_label": "Exact match on name",
                },
                {
                    "name_l": "John",
                    "name_r": "Jane",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
                {
                    "name_l": None,
                    "name_r": "John",
                    "expected_value": -1,
                    "expected_label": "name is NULL",
                },
            ],
        },
        {
            "comparison": ArrayIntersectAtSizes("tags", [3, 2, 1]),
            "inputs": [
                {
                    "tags_l": [1, 2, 3, 4],
                    "tags_r": [2, 3, 4, 5],
                    "expected_value": 3,
                    "expected_label": "Array intersection size >= 3",
                },
                {
                    "tags_l": [1, 2, 3],
                    "tags_r": [2, 3],
                    "expected_value": 2,
                    "expected_label": "Array intersection size >= 2",
                },
                {
                    "tags_l": [1],
                    "tags_r": [1],
                    "expected_value": 1,
                    "expected_label": "Array intersection size >= 1",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)
