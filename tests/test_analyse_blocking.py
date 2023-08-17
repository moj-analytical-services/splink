import duckdb
import pandas as pd

from splink.analyse_blocking import (
    cumulative_comparisons_generated_by_blocking_rules,
)
from splink.blocking import BlockingRule
from splink.duckdb.linker import DuckDBLinker

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_analyse_blocking_slow_methodology(test_helpers, dialect):
    helper = test_helpers[dialect]
    Linker = helper.Linker
    brl = helper.brl

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

    rule = brl.block_on(["first_name", "surname"])
    res = linker.count_num_comparisons_from_blocking_rule(
        rule,
    )


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
        brl.block_on(["first_name", "surname"]),
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
            "row_count": [13591, 50245, 137280],
            "cumulative_rows": [13591, 63836, 201116],
            "cartesian": 1999000,
        },
        blocking_rules=blocking_rules,
    )

    settings = {"link_type": "link_only"}
    linker_settings = Linker([df, df], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [7257, 25161, 68640],
            "cumulative_rows": [7257, 32418, 101058],
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
            "row_count": [3 * 7257, 3 * 25161, 3 * 68640],
            "cumulative_rows": [
                3 * 7257,
                3 * 7257 + 3 * 25161,
                3 * 7257 + 3 * 25161 + 3 * 68640,
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
            "row_count": [31272, 113109, 308880],
            "cumulative_rows": [31272, 31272 + 113109, 31272 + 113109 + 308880],
            "cartesian": (3000 * 2999) // 2,
        },
        blocking_rules=blocking_rules,
    )


def test_analyse_blocking_fast_methodology():
    df_1 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smith"},
            {"unique_id": 2, "first_name": "John", "surname": "Smith"},
            {"unique_id": 3, "first_name": "John", "surname": "Jones"},
            {"unique_id": 4, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 5, "first_name": "Brian", "surname": "Taylor"},
        ]
    )

    df_2 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smith"},
            {"unique_id": 2, "first_name": "John", "surname": "Smith"},
            {"unique_id": 3, "first_name": "John", "surname": "Jones"},
        ]
    )
    settings = {"link_type": "dedupe_only"}
    linker = DuckDBLinker(df_1, settings)

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "1=1",
    )
    assert res == 5 * 5

    settings = {"link_type": "dedupe_only"}
    linker = DuckDBLinker(df_1, settings)

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "l.first_name = r.first_name OR l.surname = r.surname",
    )
    assert res == 5 * 5

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "l.first_name = r.first_name AND levenshtein(l.surname, r.surname) <2",
    )
    assert res == 3 * 3 + 1 * 1 + 1 * 1

    settings = {"link_type": "link_and_dedupe"}
    linker = DuckDBLinker([df_1, df_2], settings)

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "l.first_name = r.first_name"
    )
    assert res == 6 * 6 + 1 * 1 + 1 * 1

    settings = {"link_type": "link_only"}
    linker = DuckDBLinker([df_1, df_2], settings)

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "l.first_name = r.first_name"
    )
    assert res == 3 * 3

    # Test a series of blocking rules with different edge cases.
    # Assert that the naive methodology gives the same result as the new methodlogy

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    blocking_rules = [
        "l.first_name = r.first_name",
        "l.first_name = r.first_name AND l.surname = r.surname",
        "substr(l.first_name,2,3) = substr(r.first_name,3,4)",
        "substr(l.first_name,1,1) = substr(r.surname,1,1) and l.dob = r.dob",
        "l.first_name = r.first_name and levenshtein(l.dob, r.dob) > -1",
        "l.dob = r.dob and substr(l.first_name,2,3) = substr(r.first_name,3,4)",
    ]

    sql_template = """
    select count(*)
    from df as l
    inner join df as r
    on {blocking_rule}
    """

    results = {}
    for br in blocking_rules:
        sql = sql_template.format(blocking_rule=br)
        res = duckdb.sql(sql).df()
        results[br] = {"count_from_join_dedupe_only": res.iloc[0][0]}

    linker = DuckDBLinker(df, {"link_type": "dedupe_only"})
    for br in blocking_rules:
        c = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(br)
        results[br]["count_from_efficient_fn_dedupe_only"] = c

    for br in blocking_rules:
        assert (
            results[br]["count_from_join_dedupe_only"]
            == results[br]["count_from_efficient_fn_dedupe_only"]
        )

    # Link only
    df_l = df.iloc[::2].copy()  # even-indexed rows (starting from 0)
    df_r = df.iloc[1::2].copy()  # odd-indexed rows (starting from 1)

    sql_template = """
    select count(*)
    from df_l as l
    inner join df_r as r
    on {blocking_rule}
    """

    results = {}
    for br in blocking_rules:
        sql = sql_template.format(blocking_rule=br)
        res = duckdb.sql(sql).df()
        results[br] = {"count_from_join_link_only": res.iloc[0][0]}

    linker = DuckDBLinker([df_l, df_r], {"link_type": "link_only"})
    for br in blocking_rules:
        c = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(br)
        results[br]["count_from_efficient_fn_link_only"] = c

    for br in blocking_rules:
        assert (
            results[br]["count_from_join_link_only"]
            == results[br]["count_from_efficient_fn_link_only"]
        )


def test_blocking_rule_accepts_different_dialects():
    br = "l.first_name = r.first_name"
    br = BlockingRule(br, sqlglot_dialect="spark")
    assert br._equi_join_conditions == [("first_name", "first_name")]

    br = "l.`hi THERE` = r.`hi THERE`"
    br = BlockingRule(br, sqlglot_dialect="spark")

    assert br._equi_join_conditions == [("`hi THERE`", "`hi THERE`")]


@mark_with_dialects_excluding()
def test_cumulative_br_funs(test_helpers, dialect):
    helper = test_helpers[dialect]
    Linker = helper.Linker
    brl = helper.brl
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = Linker(df, get_settings_dict(), **helper.extra_linker_args())
    linker.cumulative_comparisons_from_blocking_rules_records()
    linker.cumulative_comparisons_from_blocking_rules_records(
        [
            "l.first_name = r.first_name",
            brl.exact_match_rule("surname"),
        ]
    )

    linker.cumulative_num_comparisons_from_blocking_rules_chart(
        [
            "l.first_name = r.first_name",
            brl.exact_match_rule("surname"),
        ]
    )

    assert (
        linker.count_num_comparisons_from_blocking_rule(brl.exact_match_rule("surname"))
        == 3167
    )
