import pandas as pd

from splink.analyse_blocking import (
    cumulative_comparisons_generated_by_blocking_rules,
)

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_analyse_blocking(test_helpers, dialect):

    helper = test_helpers[dialect]
    Linker = helper.Linker

    df_1 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smith"},
            {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
            {"unique_id": 4, "first_name": "John", "surname": "Brown"},
        ]
    )

    df_2 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
            {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 3, "first_name": "Jayne", "surname": "Tailor"},
        ]
    )
    settings = {"link_type": "dedupe_only"}
    linker = Linker(df_1, settings, **helper.extra_linker_args())

    res = linker.count_num_comparisons_from_blocking_rule(
        "1=1",
    )
    assert res == 4 * 3 / 2

    res = linker.count_num_comparisons_from_blocking_rule(
        "l.first_name = r.first_name",
    )
    assert res == 1

    settings = {"link_type": "link_only"}
    linker = Linker([df_1, df_2], settings, **helper.extra_linker_args())
    res = linker.count_num_comparisons_from_blocking_rule(
        "1=1",
    )
    assert res == 4 * 3

    res = linker.count_num_comparisons_from_blocking_rule(
        "l.surname = r.surname",
    )
    assert res == 1

    res = linker.count_num_comparisons_from_blocking_rule(
        "l.first_name = r.first_name",
    )
    assert res == 3

    settings = {"link_type": "link_and_dedupe"}

    linker = Linker([df_1, df_2], settings, **helper.extra_linker_args())

    res = linker.count_num_comparisons_from_blocking_rule(
        "1=1",
    )
    expected = 4 * 3 + (4 * 3 / 2) + (3 * 2 / 2)
    assert res == expected

    rule = "l.first_name = r.first_name and l.surname = r.surname"
    res = linker.count_num_comparisons_from_blocking_rule(
        rule,
    )

    assert res == 1


def validate_blocking_output(linker, expected_out, **kwargs):
    records = cumulative_comparisons_generated_by_blocking_rules(linker, **kwargs)

    assert expected_out["row_count"] == list(map(lambda x: x["row_count"], records))

    assert expected_out["cumulative_rows"] == list(
        map(lambda x: x["cumulative_rows"], records)
    )

    assert expected_out["cartesian"] == records[0]["cartesian"]


@mark_with_dialects_excluding()
def test_blocking_records_accuracy(test_helpers, dialect):
    from numpy import nan

    helper = test_helpers[dialect]
    Linker = helper.Linker
    brl = helper.brl
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    # resolve an issue w/ pyspark nulls
    df = df.fillna(nan).replace([nan], [None])

    linker_settings = Linker(df, get_settings_dict(), **helper.extra_linker_args())

    # dedupe only
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [3167],
            "cumulative_rows": [3167],
            "cartesian": 499500,
        },
        blocking_rules=None,
    )

    # dedupe only with additional brs
    blocking_rules = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
    ]

    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [3167, 1654],
            "cumulative_rows": [3167, 4821],
            "cartesian": 499500,
        },
        blocking_rules=blocking_rules,
    )

    blocking_rules = [
        brl.exact_match_rule("first_name"),
        brl.and_(
            brl.exact_match_rule("first_name"),
            brl.exact_match_rule("surname"),
        ),
        "l.dob = r.dob",
    ]

    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [2253, 0, 1244],
            "cumulative_rows": [2253, 2253, 3497],
            "cartesian": 499500,
        },
        blocking_rules=blocking_rules,
    )

    # link and dedupe + link only without settings
    blocking_rules = [
        "l.surname = r.surname",
        brl.or_(
            brl.exact_match_rule("first_name"),
            "substr(l.dob,1,4) = substr(r.dob,1,4)",
        ),
        "l.city = r.city",
    ]

    settings = {"link_type": "link_and_dedupe"}
    linker_settings = Linker([df, df], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [13591, 53472, 137280],
            "cumulative_rows": [13591, 67063, 204343],
            "cartesian": 1999000,
        },
        blocking_rules=blocking_rules,
    )

    settings = {"link_type": "link_only"}
    linker_settings = Linker([df, df], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [7257, 27190, 68640],
            "cumulative_rows": [7257, 34447, 103087],
            "cartesian": 1000000,
        },
        blocking_rules=blocking_rules,
    )

    # now multi-table
    # still link only
    linker_settings = Linker([df, df, df], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            # number of links per block simply related to two-frame case
            "row_count": [3 * 7257, 3 * 27190, 3 * 68640],
            "cumulative_rows": [
                3 * 7257,
                3 * 7257 + 3 * 27190,
                3 * 7257 + 3 * 27190 + 3 * 68640,
            ],
            "cartesian": 3_000_000,
        },
        blocking_rules=blocking_rules,
    )

    settings = {"link_type": "link_and_dedupe"}
    linker_settings = Linker([df, df, df], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            # and as above,
            "row_count": [31272, 120993, 308880],
            "cumulative_rows": [31272, 31272 + 120993, 31272 + 120993 + 308880],
            "cartesian": (3000 * 2999) // 2,
        },
        blocking_rules=blocking_rules,
    )
