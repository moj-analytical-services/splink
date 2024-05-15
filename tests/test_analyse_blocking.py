import duckdb
import pandas as pd

from splink.blocking import BlockingRule
from splink.blocking_analysis import (
    count_comparisons_from_blocking_rule,
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
    cumulative_comparisons_to_be_scored_from_blocking_rules_data,
)
from splink.blocking_rule_library import CustomRule, Or, block_on
from splink.duckdb.database_api import DuckDBAPI

from .decorator import mark_with_dialects_excluding, mark_with_dialects_including


@mark_with_dialects_excluding()
def test_analyse_blocking_slow_methodology(test_helpers, dialect):
    helper = test_helpers[dialect]

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

    df_3 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smith"},
            {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        ]
    )

    db_api = helper.DatabaseAPI(**helper.db_api_args())
    args = {
        "link_type": "dedupe_only",
        "db_api": db_api,
        "unique_id_column_name": "unique_id",
    }

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=df_1, blocking_rule_creator="1=1", **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 4 * 3 / 2

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=df_1, blocking_rule_creator=block_on("first_name"), **args
    )

    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 1

    args["link_type"] = "link_only"
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule_creator="1=1", **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]

    assert res == 4 * 3

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule_creator=block_on("surname"), **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 1

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2],
        blocking_rule_creator=block_on("first_name"),
        **args,
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 3

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2, df_3], blocking_rule_creator="1=1", **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 4 * 3 + 4 * 2 + 2 * 3

    args["link_type"] = "link_and_dedupe"
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule_creator="1=1", **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    expected = 4 * 3 + (4 * 3 / 2) + (3 * 2 / 2)
    assert res == expected

    rule = "l.first_name = r.first_name and l.surname = r.surname"
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule_creator=rule, **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 1

    rule = block_on("first_name", "surname")
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule_creator=rule, **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 1


def validate_blocking_output(comparison_count_args, expected_out):
    records = cumulative_comparisons_to_be_scored_from_blocking_rules_data(
        **comparison_count_args
    ).to_dict(orient="records")

    assert expected_out["row_count"] == list(map(lambda x: x["row_count"], records))

    assert expected_out["cumulative_rows"] == list(
        map(lambda x: x["cumulative_rows"], records)
    )

    assert expected_out["cartesian"] == records[0]["cartesian"]


@mark_with_dialects_excluding()
def test_source_dataset_works_as_expected(test_helpers, dialect):
    helper = test_helpers[dialect]
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
    df_1["src_dataset"] = "df_1"
    df_2["src_dataset"] = "df_2"
    df_concat = pd.concat([df_1.copy(), df_2.copy()])
    df_1.drop(columns=["src_dataset"], inplace=True)
    df_2.drop(columns=["src_dataset"], inplace=True)

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    r1 = cumulative_comparisons_to_be_scored_from_blocking_rules_data(
        table_or_tables=df_concat,
        blocking_rule_creators=[block_on("first_name")],
        db_api=db_api,
        unique_id_column_name="unique_id",
        source_dataset_column_name="src_dataset",
        link_type="link_only",
    )

    r2 = cumulative_comparisons_to_be_scored_from_blocking_rules_data(
        table_or_tables=[df_1, df_2],
        blocking_rule_creators=[block_on("first_name")],
        db_api=db_api,
        unique_id_column_name="unique_id",
        link_type="link_only",
        source_dataset_column_name="source_dataset",
    )
    assert r1.to_dict(orient="records") == r2.to_dict(orient="records")


@mark_with_dialects_excluding()
def test_blocking_records_accuracy(test_helpers, dialect):
    from numpy import nan

    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    # resolve an issue w/ pyspark nulls

    df = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
        {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
        {"unique_id": 3, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
        {"unique_id": 4, "first_name": "Kim", "surname": "Lee", "dob": None},
    ]
    df = pd.DataFrame(df).fillna(nan).replace([nan], [None])

    comparison_count_args = {
        "table_or_tables": df,
        "blocking_rule_creators": [block_on("first_name")],
        "link_type": "dedupe_only",
        "db_api": db_api,
        "unique_id_column_name": "unique_id",
    }

    n = len(df)
    # dedupe only
    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "row_count": [1],
            "cumulative_rows": [1],
            "cartesian": n * (n - 1) / 2,
        },
    )

    # dedupe only with additional brs
    blocking_rules = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
    ]

    comparison_count_args["blocking_rule_creators"] = blocking_rules

    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "row_count": [1, 1],
            "cumulative_rows": [1, 2],
            "cartesian": n * (n - 1) / 2,
        },
    )

    blocking_rules = [
        block_on("first_name"),
        block_on("first_name", "surname"),
        "l.dob = r.dob",
    ]

    comparison_count_args["blocking_rule_creators"] = blocking_rules

    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "row_count": [1, 0, 1],
            "cumulative_rows": [1, 1, 2],
            "cartesian": n * (n - 1) / 2,
        },
    )

    # link and dedupe + link only
    comparison_count_args["source_dataset_column_name"] = "source_dataset"

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
        ),  # 1r:1r, 1l:2l, 1l:2r
        "l.surname = r.surname",
    ]

    comparison_count_args = {
        "table_or_tables": [df_l, df_r],
        "link_type": "link_and_dedupe",
        "db_api": db_api,
        "unique_id_column_name": "unique_id",
        "blocking_rule_creators": blocking_rules,
        "source_dataset_column_name": "source_dataset",
    }

    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "row_count": [1, 3, 0],
            "cumulative_rows": [1, 4, 4],
            "cartesian": 1 + 1 + 4,  # within, within, between
        },
    )

    blocking_rules = [
        "l.surname = r.surname",  # 2l:2r,
        Or(
            block_on("first_name"),
            CustomRule("substr(l.dob,1,4) = substr(r.dob,1,4)"),
        ),  # 1l:1r, 1l:2r
        "l.surname = r.surname",
    ]

    comparison_count_args["link_type"] = "link_only"
    comparison_count_args["blocking_rule_creators"] = blocking_rules

    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "row_count": [1, 2, 0],
            "cumulative_rows": [1, 3, 3],
            "cartesian": 4,
        },
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

    comparison_count_args = {
        "table_or_tables": [df_1, df_2, df_3],
        "link_type": "link_and_dedupe",
        "db_api": db_api,
        "unique_id_column_name": "unique_id",
        "blocking_rule_creators": [
            block_on("surname"),
            block_on("first_name"),
        ],
        "source_dataset_column_name": "source_dataset",
    }

    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "row_count": [2, 2],
            "cumulative_rows": [2, 4],
            "cartesian": 5 * 4 / 2,
        },
    )

    comparison_count_args["link_type"] = "link_only"
    comparison_count_args["blocking_rule_creators"] = [
        block_on("surname"),
        block_on("first_name"),
    ]

    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "row_count": [2, 2],
            "cumulative_rows": [2, 4],
            "cartesian": 8,
        },
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

    db_api = DuckDBAPI()

    args = {
        "table_or_tables": df_1,
        "link_type": "dedupe_only",
        "db_api": db_api,
        "unique_id_column_name": "unique_id",
        "compute_post_filter_count": False,
    }

    args["blocking_rule_creator"] = "1=1"

    res_dict = count_comparisons_from_blocking_rule(**args)

    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]

    assert res == 5 * 5

    args["blocking_rule_creator"] = (
        "l.first_name = r.first_name OR l.surname = r.surname"
    )
    res_dict = count_comparisons_from_blocking_rule(**args)
    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]
    assert res == 5 * 5

    #     res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
    #         "l.first_name = r.first_name AND levenshtein(l.surname, r.surname) <2",
    #     )
    #     assert res == 3 * 3 + 1 * 1 + 1 * 1

    args["blocking_rule_creator"] = """l.first_name = r.first_name
                                AND levenshtein(l.surname, r.surname) <2"""
    res_dict = count_comparisons_from_blocking_rule(**args)
    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]
    assert res == 3 * 3 + 1 * 1 + 1 * 1

    args["table_or_tables"] = [df_1, df_2]
    args["link_type"] = "link_and_dedupe"
    args["blocking_rule_creator"] = block_on("first_name")

    res_dict = count_comparisons_from_blocking_rule(**args)
    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]

    assert res == 6 * 6 + 1 * 1 + 1 * 1

    args["link_type"] = "link_only"
    args["blocking_rule_creator"] = block_on("first_name")

    res_dict = count_comparisons_from_blocking_rule(**args)
    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]
    assert res == 3 * 3


@mark_with_dialects_including("duckdb")
def test_analyse_blocking_fast_methodology_edge_cases():
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

    for br in blocking_rules:
        res_dict = count_comparisons_from_blocking_rule(
            table_or_tables=df,
            blocking_rule_creator=br,
            link_type="dedupe_only",
            db_api=db_api,
            unique_id_column_name="unique_id",
        )
        c = res_dict["number_of_comparisons_generated_pre_filter_conditions"]

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

    for br in blocking_rules:
        res_dict = count_comparisons_from_blocking_rule(
            table_or_tables=[df_l, df_r],
            blocking_rule_creator=br,
            link_type="link_only",
            db_api=db_api,
            unique_id_column_name="unique_id",
        )
        c = res_dict["number_of_comparisons_generated_pre_filter_conditions"]

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
def test_chart(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
        table_or_tables=df,
        blocking_rule_creators=[block_on("first_name"), "l.surname = r.surname"],
        link_type="dedupe_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
    )
