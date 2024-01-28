import duckdb
import pandas as pd

from splink.analyse_blocking import cumulative_comparisons_generated_by_blocking_rules
from splink.blocking import BlockingRule
from splink.blocking_rule_library import CustomRule, Or, block_on
from splink.database_api import DuckDBAPI
from splink.linker import Linker

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_analyse_blocking_slow_methodology(test_helpers, dialect):
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

    rule = block_on("first_name", "surname").get_blocking_rule(dialect)
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

    # resolve an issue w/ pyspark nulls

    df = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
        {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
        {"unique_id": 3, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
        {"unique_id": 4, "first_name": "Kim", "surname": "Lee", "dob": None},
    ]
    df = pd.DataFrame(df).fillna(nan).replace([nan], [None])

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
        ],
        "comparisons": [],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
        "em_convergence": 0.001,
        "max_iterations": 20,
    }

    linker_settings = Linker(df, settings, **helper.extra_linker_args())
    n = len(df)
    # dedupe only
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [1],
            "cumulative_rows": [1],
            "cartesian": n * (n - 1) / 2,
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
            "row_count": [1, 1],
            "cumulative_rows": [1, 2],
            "cartesian": n * (n - 1) / 2,
        },
        blocking_rules=blocking_rules,
    )

    blocking_rules = [
        block_on("first_name").get_blocking_rule(dialect),
        block_on("first_name", "surname").get_blocking_rule(dialect),
        "l.dob = r.dob",
    ]

    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [1, 0, 1],
            "cumulative_rows": [1, 1, 2],
            "cartesian": n * (n - 1) / 2,
        },
        blocking_rules=blocking_rules,
    )

    # link and dedupe + link only
    df_l = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
        {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
    ]

    df_l = pd.DataFrame(df_l)

    df_r = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
        {"unique_id": 2, "first_name": "Kim", "surname": "Lee", "dob": None},
    ]

    df_r = pd.DataFrame(df_r).fillna(nan).replace([nan], [None])

    blocking_rules = [
        "l.surname = r.surname",  # 2l:2r,
        Or(
            block_on("first_name"), CustomRule("substr(l.dob,1,4) = substr(r.dob,1,4)")
        ).get_blocking_rule(
            dialect
        ),  # 1r:1r, 1l:2l, 1l:2r
        "l.surname = r.surname",
    ]

    settings = {"link_type": "link_and_dedupe"}
    linker_settings = Linker([df_l, df_r], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [1, 3, 0],
            "cumulative_rows": [1, 4, 4],
            "cartesian": 1 + 1 + 4,  # within, within, between
        },
        blocking_rules=blocking_rules,
    )

    blocking_rules = [
        "l.surname = r.surname",  # 2l:2r,
        Or(
            block_on("first_name"),
            CustomRule("substr(l.dob,1,4) = substr(r.dob,1,4)"),
        ).get_blocking_rule(
            dialect
        ),  # 1l:1r, 1l:2r
        "l.surname = r.surname",
    ]

    settings = {"link_type": "link_only"}
    linker_settings = Linker([df_l, df_r], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [1, 2, 0],
            "cumulative_rows": [1, 3, 3],
            "cartesian": 4,
        },
        blocking_rules=blocking_rules,
    )

    # link and dedupe
    df_1 = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
        {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
    ]

    df_1 = pd.DataFrame(df_l)

    df_2 = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
        {"unique_id": 2, "first_name": "Kim", "surname": "Lee", "dob": None},
    ]

    df_2 = pd.DataFrame(df_2).fillna(nan).replace([nan], [None])

    df_3 = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
    ]

    df_3 = pd.DataFrame(df_3)

    settings = {"link_type": "link_and_dedupe"}
    blocking_rules = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
    ]

    linker_settings = Linker([df_1, df_2, df_3], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [2, 2],
            "cumulative_rows": [2, 4],
            "cartesian": 5 * 4 / 2,
        },
        blocking_rules=blocking_rules,
    )

    settings = {"link_type": "link_only"}
    blocking_rules = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
    ]

    linker_settings = Linker([df_1, df_2, df_3], settings, **helper.extra_linker_args())
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [2, 2],
            "cumulative_rows": [2, 4],
            "cartesian": 8,
        },
        blocking_rules=blocking_rules,
    )

    blocking_rules_df = cumulative_comparisons_generated_by_blocking_rules(
        linker_settings, blocking_rules=blocking_rules, return_dataframe=True
    )

    expected_row_count = pd.DataFrame({"row_count": [2, 2]})
    assert (blocking_rules_df["row_count"] == expected_row_count["row_count"]).all()


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
    db_api = DuckDBAPI()

    linker = Linker(df_1, settings, database_api=db_api)

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "1=1",
    )
    assert res == 5 * 5

    settings = {"link_type": "dedupe_only"}
    db_api = DuckDBAPI()

    linker = Linker(df_1, settings, database_api=db_api)

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "l.first_name = r.first_name OR l.surname = r.surname",
    )
    assert res == 5 * 5

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "l.first_name = r.first_name AND levenshtein(l.surname, r.surname) <2",
    )
    assert res == 3 * 3 + 1 * 1 + 1 * 1

    settings = {"link_type": "link_and_dedupe"}
    db_api = DuckDBAPI()

    linker = Linker([df_1, df_2], settings, database_api=db_api)

    res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
        "l.first_name = r.first_name"
    )
    assert res == 6 * 6 + 1 * 1 + 1 * 1

    settings = {"link_type": "link_only"}
    db_api = DuckDBAPI()

    linker = Linker([df_1, df_2], settings, database_api=db_api)

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

    db_api = DuckDBAPI()

    linker = Linker(df, {"link_type": "dedupe_only"}, database_api=db_api)
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

    db_api = DuckDBAPI()

    linker = Linker([df_l, df_r], {"link_type": "link_only"}, database_api=db_api)
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

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = Linker(df, get_settings_dict(), **helper.extra_linker_args())
    linker.cumulative_comparisons_from_blocking_rules_records()
    linker.cumulative_comparisons_from_blocking_rules_records(
        [
            "l.first_name = r.first_name",
            block_on("surname").get_blocking_rule(dialect),
        ]
    )

    linker.cumulative_num_comparisons_from_blocking_rules_chart(
        [
            "l.first_name = r.first_name",
            block_on("surname").get_blocking_rule(dialect),
        ]
    )

    assert (
        linker.count_num_comparisons_from_blocking_rule(
            block_on("surname").get_blocking_rule(dialect)
        )
        == 3167
    )
