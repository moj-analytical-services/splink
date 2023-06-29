import pandas as pd
import pytest

from tests.decorator import mark_with_dialects_excluding


def postcode(num):
    return f"XX{num} {num}YZ"


# No SQLite - no array comparisons in library
@mark_with_dialects_excluding("sqlite")
def test_array_comparisons(test_helpers, dialect):
    helper = test_helpers[dialect]
    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "first_name": "Isabella",
                "postcode": [postcode(1), postcode(2)],
            },
            {
                "unique_id": 2,
                "first_name": "Mary",
                "postcode": [postcode(1), postcode(3), postcode(7)],
            },
            {
                "unique_id": 3,
                "first_name": "Hannah",
                "postcode": [postcode(1), postcode(2), postcode(4)],
            },
            {
                "unique_id": 4,
                "first_name": "Jane",
                "postcode": [postcode(1), postcode(2), postcode(4)],
            },
            {
                "unique_id": 5,
                "first_name": "Isabella",
                "postcode": [postcode(5), postcode(6)],
            },
            {
                "unique_id": 6,
                "first_name": "Graham",
                "postcode": [postcode(7), postcode(8), postcode(9), postcode(10)],
            },
            {
                "unique_id": 7,
                "first_name": "Mark",
                "postcode": [postcode(7), postcode(8), postcode(9), postcode(10)],
            },
        ]
    )
    df = helper.convert_frame(df)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            helper.cl.array_intersect_at_sizes("postcode", [4, 3, 2, 1]),
            helper.cl.exact_match("first_name"),
        ],
    }
    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.predict().as_pandas_dataframe()

    # ID pairs with various sizes of intersections
    intersection_size_ids = {
        4: {(6, 7)},
        3: {(3, 4)},
        2: {(1, 3), (1, 4)},
        1: {(1, 2), (2, 3), (2, 4), (2, 6), (2, 7)},
    }
    postcode_comparison = linker._settings_obj.comparisons[0]
    # size: gamma_level value
    size_gamma_lookup = {1: 1, 2: 2, 3: 3, 4: 4}

    for size, id_comb in intersection_size_ids.items():
        gamma_val = size_gamma_lookup[size]
        postcode_comparison._get_comparison_level_by_comparison_vector_value(
            gamma_val
        ).as_dict()
        for id_pair in id_comb:
            row = dict(
                df_e.query(
                    "unique_id_l == {} and unique_id_r == {}".format(*id_pair)
                ).iloc[0]
            )
            assert row["gamma_postcode"] == gamma_val

    # and again but with coarser levels
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            helper.cl.array_intersect_at_sizes("postcode", [3, 1]),
            helper.cl.exact_match("first_name"),
        ],
    }
    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.predict().as_pandas_dataframe()

    # now levels encompass multiple size intersections
    intersection_size_ids = {
        3: intersection_size_ids[3].union(intersection_size_ids[4]),
        1: intersection_size_ids[1].union(intersection_size_ids[2]),
    }
    postcode_comparison = linker._settings_obj.comparisons[0]
    # size: gamma_level value
    size_gamma_lookup = {1: 1, 3: 2}

    for size, id_comb in intersection_size_ids.items():
        gamma_val = size_gamma_lookup[size]
        postcode_comparison._get_comparison_level_by_comparison_vector_value(
            gamma_val
        ).as_dict()
        for id_pair in id_comb:
            row = dict(
                df_e.query(
                    "unique_id_l == {} and unique_id_r == {}".format(*id_pair)
                ).iloc[0]
            )
            assert row["gamma_postcode"] == gamma_val

    # check we get an error if we try to pass -ve sizes
    with pytest.raises(ValueError):
        settings = {
            "link_type": "dedupe_only",
            "comparisons": [
                helper.cl.array_intersect_at_sizes("postcode", [-1, 2]),
                helper.cl.exact_match("first_name"),
            ],
        }
