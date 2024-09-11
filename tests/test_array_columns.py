import pytest

import splink.internals.comparison_library as cl
import splink.internals.comparison_level_library as cll
from tests.decorator import mark_with_dialects_excluding
from tests.literal_utils import (
    ComparisonTestSpec,
    ComparisonLevelTestSpec,
    LiteralTestValues,
    run_tests_with_args,
)


# No SQLite - no array comparisons in library
@mark_with_dialects_excluding("sqlite", "spark")
def test_array_comparison_1(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_spec = ComparisonTestSpec(
        cl.ArrayIntersectAtSizes("arr", [4, 3, 2, 1]),
        tests=[
            LiteralTestValues(
                {"arr_l": ["A", "B", "C", "D"], "arr_r": ["A", "B", "C", "D"]},
                expected_gamma_val=4,
            ),
            LiteralTestValues(
                {"arr_l": ["A", "B", "C", "D"], "arr_r": ["A", "B", "C", "Z"]},
                expected_gamma_val=3,
            ),
            LiteralTestValues(
                {"arr_l": ["A", "B"], "arr_r": ["A", "B", "C", "D"]},
                expected_gamma_val=2,
            ),
            LiteralTestValues(
                {"arr_l": ["A", "B", "C", "D"], "arr_r": ["X", "Y", "Z"]},
                expected_gamma_val=0,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonTestSpec(
        cl.ArrayIntersectAtSizes("arr", [4, 1]),
        tests=[
            LiteralTestValues(
                {"arr_l": ["A", "B", "C", "D"], "arr_r": ["A", "B", "C", "D"]},
                expected_gamma_val=2,
            ),
            LiteralTestValues(
                {"arr_l": ["A", "B", "C", "D"], "arr_r": ["A", "B", "C", "Z"]},
                expected_gamma_val=1,
            ),
            LiteralTestValues(
                {"arr_l": ["A", "B"], "arr_r": ["A", "B", "C", "D"]},
                expected_gamma_val=1,
            ),
            LiteralTestValues(
                {"arr_l": ["A"], "arr_r": ["X", "Y", "Z"]},
                expected_gamma_val=0,
            ),
            # This fails with postgres because it can't infer the type of
            # the empty array (is it an array of char, int etc.)
            # LiteralTestValues(
            #     {"arr_l": [], "arr_r": ["X", "Y", "Z"]},
            #     expected_gamma_val=0,
            # ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    # check we get an error if we try to pass -ve sizes
    with pytest.raises(ValueError):
        cl.ArrayIntersectAtSizes("postcode", [-1, 2]).get_comparison(
            db_api.sql_dialect.sqlglot_name
        )


# No SQLite - no array comparisons in library
@mark_with_dialects_excluding("sqlite")
def test_array_subset(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_spec = ComparisonLevelTestSpec(
        cll.ArraySubsetLevel("arr"),
        tests=[
            LiteralTestValues(
                {"arr_l": ["A", "B", "C", "D"], "arr_r": ["A", "B", "C", "D"]},
                expected_in_level=True,
            ),
            LiteralTestValues(
                {"arr_l": ["A", "B", "C", "D"], "arr_r": ["A", "B", "C", "Z"]},
                expected_in_level=False,
            ),
            LiteralTestValues(
                {"arr_l": ["A", "B"], "arr_r": ["A", "B", "C", "D"]},
                expected_in_level=True,
            ),
            LiteralTestValues(
                {"arr_l": ["A", "B", "C", "D"], "arr_r": ["X", "Y", "Z"]},
                expected_in_level=False,
            ),
            LiteralTestValues(
                {"arr_l": [], "arr_r": ["X", "Y", "Z"]},
                expected_in_level=False,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)
