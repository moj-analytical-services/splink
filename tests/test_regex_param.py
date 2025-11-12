import pandas as pd
import pytest

import splink.internals.comparison_level_library as cll
from splink.internals.column_expression import ColumnExpression

from .decorator import mark_with_dialects_excluding

df_pandas = pd.DataFrame(
    [
        {
            "unique_id": 1,
            "first_name": "Andy",
            "last_name": "Williams",
            "postcode": "SE1P 0NY",
        },
        {
            "unique_id": 2,
            "first_name": "Andy's twin",
            "last_name": "Williams",
            "postcode": "SE1P 0NY",
        },
        {
            "unique_id": 3,
            "first_name": "Tom",
            "last_name": "Williams",
            "postcode": "SE1P 0PZ",
        },
        {
            "unique_id": 4,
            "first_name": "Robin",
            "last_name": "Williams",
            "postcode": "SE1P 4UY",
        },
        {
            "unique_id": 5,
            "first_name": "Sam",
            "last_name": "Rosston",
            "postcode": "SE2 7TR",
        },
        {
            "unique_id": 6,
            "first_name": "Ross",
            "last_name": "Samson",
            "postcode": "SW15 8UY",
        },
    ]
)


def postcode_levels():
    return {
        "output_column_name": "postcode",
        "comparison_levels": [
            cll.ExactMatchLevel(
                ColumnExpression("postcode").regex_extract(
                    "^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9]"
                )
            ),
            cll.LevenshteinLevel(
                ColumnExpression("postcode").regex_extract("^[A-Z]{1,2}[0-9][A-Z0-9]?"),
                distance_threshold=1,
            ),
            cll.JaroLevel(
                ColumnExpression("postcode").regex_extract("^[A-Z]{1,2}"),
                distance_threshold=1,
            ),
            cll.ElseLevel(),
        ],
    }


def name_levels():
    return {
        "output_column_name": "name",
        "comparison_levels": [
            cll.JaroWinklerLevel(
                ColumnExpression("first_name").regex_extract("^[A-Z]{1,4}"),
                distance_threshold=1,
            ),
            cll.ColumnsReversedLevel(
                ColumnExpression("first_name").regex_extract("[A-Z]{1,3}"),
                ColumnExpression("last_name").regex_extract("[A-Z]{1,3}"),
            ),
            cll.ElseLevel(),
        ],
    }


record_pairs_gamma_postcode = {
    3: [(1, 2), (1, 3), (2, 3)],
    2: [(1, 4), (2, 4), (3, 4)],
    1: [(1, 5), (2, 5), (3, 5), (4, 5)],
}

record_pairs_gamma_name = {
    2: [(1, 2), (4, 6)],
    1: [(5, 6)],
}


# TODO: we can restore postgres if we translate from Jaro + JaroWinkler
# which should be okay as these are not crucial to the test
@mark_with_dialects_excluding("postgres", "sqlite")
@pytest.mark.parametrize(
    ("level_set", "record_pairs_gamma"),
    [
        pytest.param(
            postcode_levels(),
            record_pairs_gamma_postcode,
            id="name regex levels test",
        ),
        pytest.param(
            name_levels(),
            record_pairs_gamma_name,
            id="name regex levels test",
        ),
    ],
)
def test_regex(dialect, test_helpers, level_set, record_pairs_gamma):
    helper = test_helpers[dialect]

    # Generate settings
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [level_set],
    }

    comparison_name = level_set["output_column_name"]

    df = helper.convert_frame(df_pandas)
    linker = helper.Linker(df, settings, **helper.extra_linker_args())

    linker_output = linker.inference.predict().as_pandas_dataframe()

    for gamma, id_pairs in record_pairs_gamma.items():
        for left, right in id_pairs:
            assert (
                linker_output.loc[
                    (linker_output.unique_id_l == left)
                    & (linker_output.unique_id_r == right)
                ][f"gamma_{comparison_name}"].values[0]
                == gamma
            )


# TODO: previously this checked validity of syntax wrt Spark dialect
# do we still want such functionality?
# def test_invalid_regex():
#     cll.ExactMatchLevel(ColumnExpression("postcode").regex_extract("^[A-Z]\\d"))
#     cll.ExactMatchLevel(ColumnExpression("postcode").regex_extract("^[A-Z]{1}"))
#     with pytest.raises(SyntaxError):
#         cll.ExactMatchLevel(ColumnExpression("postcode").regex_extract("^[A-Z]\\d"))
