import pytest

from splink.comparison_library import ArrayIntersectAtSizes
from splink.internals.testing import comparison_vector_value
from tests.decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding("sqlite", "spark")
def test_array_comparison_1(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": ArrayIntersectAtSizes("arr", [4, 3, 2, 1]),
            "inputs": [
                {
                    "arr_l": ["A", "B", "C", "D"],
                    "arr_r": ["A", "B", "C", "D"],
                    "expected_value": 4,
                    "expected_label": "Array intersection size >= 4",
                },
                {
                    "arr_l": ["A", "B", "C", "D"],
                    "arr_r": ["A", "B", "C", "Z"],
                    "expected_value": 3,
                    "expected_label": "Array intersection size >= 3",
                },
                {
                    "arr_l": ["A", "B"],
                    "arr_r": ["A", "B", "C", "D"],
                    "expected_value": 2,
                    "expected_label": "Array intersection size >= 2",
                },
                {
                    "arr_l": ["A", "B", "C", "D"],
                    "arr_r": ["X", "Y", "Z"],
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
            ],
        },
        {
            "comparison": ArrayIntersectAtSizes("arr", [4, 1]),
            "inputs": [
                {
                    "arr_l": ["A", "B", "C", "D"],
                    "arr_r": ["A", "B", "C", "D"],
                    "expected_value": 2,
                    "expected_label": "Array intersection size >= 4",
                },
                {
                    "arr_l": ["A", "B", "C", "D"],
                    "arr_r": ["A", "B", "C", "Z"],
                    "expected_value": 1,
                    "expected_label": "Array intersection size >= 1",
                },
                {
                    "arr_l": ["A", "B"],
                    "arr_r": ["A", "B", "C", "D"],
                    "expected_value": 1,
                    "expected_label": "Array intersection size >= 1",
                },
                {
                    "arr_l": ["A"],
                    "arr_r": ["X", "Y", "Z"],
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
            ],
        },
    ]

    for case in test_cases:
        inputs = [
            {
                k: v
                for k, v in input_data.items()
                if k not in ["expected_value", "expected_label"]
            }
            for input_data in case["inputs"]
        ]
        expected_values = [
            input_data["expected_value"] for input_data in case["inputs"]
        ]
        expected_labels = [
            input_data["expected_label"] for input_data in case["inputs"]
        ]

        results = comparison_vector_value(case["comparison"], inputs, db_api)

        for result, expected_value, expected_label in zip(
            results, expected_values, expected_labels
        ):
            assert result["comparison_vector_value"] == expected_value
            assert result["label_for_charts"] == expected_label

    # Test for ValueError with negative sizes
    with pytest.raises(ValueError):
        ArrayIntersectAtSizes("postcode", [-1, 2]).get_comparison(
            db_api.sql_dialect.sqlglot_name
        )
