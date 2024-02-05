import splink.comparison_level_library as cll
from splink.column_expression import ColumnExpression
from tests.decorator import mark_with_dialects_excluding
from tests.literal_utils import (
    ComparisonLevelTestSpec,
    LiteralTestValues,
    run_tests_with_args,
)


@mark_with_dialects_excluding("sqlite", "postgres", "spark")
def test_datediff_levels(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    col_exp = ColumnExpression("dob").try_parse_date()
    test_spec = ComparisonLevelTestSpec(
        cll.DatediffLevel,
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
