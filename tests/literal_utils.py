from typing import Any, Dict, List

from splink import DuckDBAPI
from splink.internals.testing import comparison_vector_value, is_in_level

db_api = DuckDBAPI()


def run_is_in_level_tests(test_cases: List[Dict[str, Any]], db_api: Any) -> None:
    for case in test_cases:
        inputs = []
        expected = []

        for input_data in case["inputs"]:
            input_dict = {k: v for k, v in input_data.items() if k != "expected"}
            inputs.append(input_dict)
            expected.append(input_data["expected"])

        results = is_in_level(case["level"], inputs, db_api)
        assert (
            results == expected
        ), f"Expected {expected}, but got {results} for case: {case}"


def run_comparison_vector_value_tests(
    test_cases: List[Dict[str, Any]], db_api: Any
) -> None:
    for case in test_cases:
        inputs = []
        expected_values = []
        expected_labels = []

        for input_data in case["inputs"]:
            input_dict = {
                k: v
                for k, v in input_data.items()
                if k not in ["expected_value", "expected_label"]
            }
            inputs.append(input_dict)
            expected_values.append(input_data["expected_value"])
            expected_labels.append(input_data["expected_label"])

        results = comparison_vector_value(case["comparison"], inputs, db_api)

        for i, (result, expected_value, expected_label) in enumerate(
            zip(results, expected_values, expected_labels)
        ):
            assert result["comparison_vector_value"] == expected_value, (
                f"For case {case['comparison']} input {i}, "
                f"expected value {expected_value}, "
                f"but got {result['comparison_vector_value']}"
            )
            assert result["label_for_charts"] == expected_label, (
                f"For case {case['comparison']} input {i}, "
                f"expected label '{expected_label}', "
                f"but got '{result['label_for_charts']}'"
            )
