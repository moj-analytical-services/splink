import duckdb
import pandas as pd

import splink.blocking_rule_library as brl
from splink.blocking_analysis import (
    count_comparisons_from_blocking_rule,
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
    cumulative_comparisons_to_be_scored_from_blocking_rules_data,
    n_largest_blocks,
)
from splink.internals.blocking import BlockingRule
from splink.internals.blocking_rule_library import CustomRule, Or, block_on
from splink.internals.duckdb.database_api import DuckDBAPI

from .decorator import mark_with_dialects_excluding, mark_with_dialects_including


# This is slow in Spark, and so long as this passes in duckdb, there's no reason it
# shouldn't in Spark
@mark_with_dialects_excluding("spark")
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
        table_or_tables=df_1, blocking_rule="1=1", **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 4 * 3 / 2

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=df_1, blocking_rule=block_on("first_name"), **args
    )

    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 1

    args["link_type"] = "link_only"
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule="1=1", **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]

    assert res == 4 * 3

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule=block_on("surname"), **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 1

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2],
        blocking_rule=block_on("first_name"),
        **args,
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 3

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2, df_3], blocking_rule="1=1", **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 4 * 3 + 4 * 2 + 2 * 3

    args["link_type"] = "link_and_dedupe"
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule="1=1", **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    expected = 4 * 3 + (4 * 3 / 2) + (3 * 2 / 2)
    assert res == expected

    rule = "l.first_name = r.first_name and l.surname = r.surname"
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule=rule, **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 1

    rule = block_on("first_name", "surname")
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule=rule, **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 1


@mark_with_dialects_including("duckdb", "spark", pass_dialect=True)
def test_blocking_analysis_slow_methodology_exploding(test_helpers, dialect):
    helper = test_helpers[dialect]

    df_1 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "postcode": [1001, 1002]},
            {"unique_id": 2, "first_name": "Mary", "postcode": [1002, 1003]},
            {"unique_id": 3, "first_name": "Jane", "postcode": [1003]},
            {"unique_id": 4, "first_name": "John", "postcode": [1001]},
        ]
    )

    df_2 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "postcode": [1001, 1004]},
            {"unique_id": 2, "first_name": "Mary", "postcode": [1003, 1004]},
            {"unique_id": 3, "first_name": "Jayne", "postcode": [1003]},
        ]
    )
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    args = {
        "link_type": "link_only",
        "db_api": db_api,
        "unique_id_column_name": "unique_id",
    }

    rule = block_on("postcode", arrays_to_explode=["postcode"])
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule=rule, **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 6

    args = {
        "link_type": "link_and_dedupe",
        "db_api": db_api,
        "unique_id_column_name": "unique_id",
    }

    rule = block_on("postcode", arrays_to_explode=["postcode"])
    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule=rule, **args
    )
    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]
    assert res == 3 + 6 + 2


# Just run in duckdb for speed
@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_blocking_analysis_slow_methodology_exploding_2(test_helpers, dialect):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    cols = ("unique_id", "sds", "first_name", "postcode", "age", "amount")

    rows_1 = [
        (1, "a", "John", [1, 2], [2, 3], 5),
        (2, "a", "Mary", [10, 11, 12, 13], [11, 12], 5),
    ]
    df_1 = pd.DataFrame(rows_1, columns=cols)

    rows_2 = [
        (1, "b", "John", [1, 4], [1, 2, 3], 5),
        (2, "b", "John", [5], [1, 2, 3], 5),
        (3, "b", "John", [1], [1], 5),
        (4, "b", "John", [1], [3], 1),
        (5, "b", "Mary", [10], [11, 12], 5),
        (6, "b", "Mary", [10], [11, 12], 1),
        (7, "b", "Mary", [10, 11, 12, 13], [11, 12], 1),
    ]
    df_2 = pd.DataFrame(rows_2, columns=cols)

    args = {
        "link_type": "link_only",
        "db_api": db_api,
        "unique_id_column_name": "unique_id",
        "source_dataset_column_name": "sds",
    }

    rule = {
        "blocking_rule": """
            l.first_name = r.first_name
            and l.postcode = r.postcode
            and l.age = r.age
            and r.amount > 2
        """,
        "sql_dialect": "duckdb",
        "arrays_to_explode": ["postcode", "age"],
    }

    res_dict = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1, df_2], blocking_rule=rule, **args
    )

    sql = """
    select count(*) as count
    from df_1 as l
    cross join df_2 as r
    where
    l.first_name = r.first_name
    and len(array_intersect(l.postcode, r.postcode)) > 0
    and len(array_intersect(l.age, r.age)) > 0
    and r.amount > 2
    """

    c = duckdb.sql(sql).fetchone()[0]

    res = res_dict["number_of_comparisons_to_be_scored_post_filter_conditions"]

    assert res == c


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
        blocking_rules=[block_on("first_name")],
        db_api=db_api,
        unique_id_column_name="unique_id",
        source_dataset_column_name="src_dataset",
        link_type="link_only",
    )

    r2 = cumulative_comparisons_to_be_scored_from_blocking_rules_data(
        table_or_tables=[df_1, df_2],
        blocking_rules=[block_on("first_name")],
        db_api=db_api,
        unique_id_column_name="unique_id",
        link_type="link_only",
        source_dataset_column_name="source_dataset",
    )
    assert r1.to_dict(orient="records") == r2.to_dict(orient="records")

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df_1 = df[df["unique_id"] % 3 == 0].copy()
    df_1["sds"] = "df_1_name"
    df_2 = df[df["unique_id"] % 3 == 1].copy()
    df_2["sds"] = "df_2_name"
    df_3 = df[df["unique_id"] % 3 == 2].copy()
    df_3["sds"] = "df_3_name"

    df_concat_2 = pd.concat([df_1, df_2])
    df_concat_3 = pd.concat([df_1, df_2, df_3])

    df_1_no_sds = df[df["unique_id"] % 3 == 0].copy()
    df_2_no_sds = df[df["unique_id"] % 3 == 1].copy()
    df_3_no_sds = df[df["unique_id"] % 3 == 2].copy()

    count_comparisons_from_blocking_rule(
        table_or_tables=df_concat_3,
        blocking_rule=block_on("first_name"),
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
        db_api=db_api,
    )

    r1 = count_comparisons_from_blocking_rule(
        table_or_tables=df_concat_3,
        blocking_rule=block_on("first_name"),
        link_type="link_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
        source_dataset_column_name="sds",
    )

    r2 = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1_no_sds, df_2_no_sds, df_3_no_sds],
        blocking_rule=block_on("first_name"),
        link_type="link_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
    )
    # Both of the above use the vertical concat of the two datasets so should
    # be equivalent
    keys_to_check = [
        "number_of_comparisons_generated_pre_filter_conditions",
        "number_of_comparisons_to_be_scored_post_filter_conditions",
    ]
    for k in keys_to_check:
        assert r1[k] == r2[k]

    r1 = count_comparisons_from_blocking_rule(
        table_or_tables=df_concat_2,
        blocking_rule=block_on("first_name"),
        link_type="link_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
        source_dataset_column_name="sds",
    )

    r2 = count_comparisons_from_blocking_rule(
        table_or_tables=[df_1_no_sds, df_2_no_sds],
        blocking_rule=block_on("first_name"),
        link_type="link_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
    )
    # There's an optimisation in the case of two input dataframes only
    # so these are not the same
    assert (
        r1["number_of_comparisons_generated_pre_filter_conditions"]
        > r2["number_of_comparisons_generated_pre_filter_conditions"]
    )

    # But after filters, should be the same
    assert (
        r1["number_of_comparisons_to_be_scored_post_filter_conditions"]
        == r2["number_of_comparisons_to_be_scored_post_filter_conditions"]
    )


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
        "blocking_rules": [block_on("first_name")],
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

    comparison_count_args["blocking_rules"] = blocking_rules

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

    comparison_count_args["blocking_rules"] = blocking_rules

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
        "blocking_rules": blocking_rules,
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
    comparison_count_args["blocking_rules"] = blocking_rules

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
        "blocking_rules": [
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
    comparison_count_args["blocking_rules"] = [
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

    args["blocking_rule"] = "1=1"

    res_dict = count_comparisons_from_blocking_rule(**args)

    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]

    assert res == 5 * 5

    args["blocking_rule"] = "l.first_name = r.first_name OR l.surname = r.surname"
    res_dict = count_comparisons_from_blocking_rule(**args)
    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]
    assert res == 5 * 5

    #     res = linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(
    #         "l.first_name = r.first_name AND levenshtein(l.surname, r.surname) <2",
    #     )
    #     assert res == 3 * 3 + 1 * 1 + 1 * 1

    args["blocking_rule"] = """l.first_name = r.first_name
                                AND levenshtein(l.surname, r.surname) <2"""
    res_dict = count_comparisons_from_blocking_rule(**args)
    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]
    assert res == 3 * 3 + 1 * 1 + 1 * 1

    args["table_or_tables"] = [df_1, df_2]
    args["link_type"] = "link_and_dedupe"
    args["blocking_rule"] = block_on("first_name")

    res_dict = count_comparisons_from_blocking_rule(**args)
    res = res_dict["number_of_comparisons_generated_pre_filter_conditions"]

    assert res == 6 * 6 + 1 * 1 + 1 * 1

    args["link_type"] = "link_only"
    args["blocking_rule"] = block_on("first_name")

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
        results[br] = {"count_from_join_dedupe_only": res.iloc[0].iloc[0]}

    db_api = DuckDBAPI()

    for br in blocking_rules:
        res_dict = count_comparisons_from_blocking_rule(
            table_or_tables=df,
            blocking_rule=br,
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
        results[br] = {"count_from_join_link_only": res.iloc[0].iloc[0]}

    db_api = DuckDBAPI()

    for br in blocking_rules:
        res_dict = count_comparisons_from_blocking_rule(
            table_or_tables=[df_l, df_r],
            blocking_rule=br,
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
    br = BlockingRule(br, sql_dialect_str="spark")
    assert br._equi_join_conditions == [("first_name", "first_name")]

    br = "l.`hi THERE` = r.`hi THERE`"
    br = BlockingRule(br, sql_dialect_str="spark")

    assert br._equi_join_conditions == [("`hi THERE`", "`hi THERE`")]


@mark_with_dialects_excluding()
def test_chart(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
        table_or_tables=df,
        blocking_rules=[block_on("first_name"), "l.surname = r.surname"],
        link_type="dedupe_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
    )


@mark_with_dialects_excluding()
def test_n_largest_blocks(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    df_1 = pd.DataFrame(
        [
            {"unique_id": 1, "name1": "Mary", "name2": "Jones", "dob": "2024-07-02"},
            {"unique_id": 2, "name1": "Mary", "name2": "Jones", "dob": "2024-07-02"},
            {"unique_id": 3, "name1": "Mary", "name2": "Jones", "dob": "2024-11-28"},
            {"unique_id": 4, "name1": "Maurice", "name2": "Jones", "dob": "2024-07-02"},
            {"unique_id": 5, "name1": "Jones", "name2": "Maurice", "dob": "2024-07-02"},
            {"unique_id": 6, "name1": "Jones", "name2": "Maurice", "dob": "2024-07-02"},
        ]
    )

    df_2 = pd.DataFrame(
        [
            {"unique_id": 1, "name1": "Mary", "name2": "Jones", "dob": "2024-07-02"},
            {"unique_id": 2, "name1": "Mary", "name2": "Jones", "dob": "2024-07-02"},
            {"unique_id": 3, "name1": "Mary", "name2": "Jones", "dob": "2024-11-28"},
            {"unique_id": 4, "name1": "Jones", "name2": "Maurice", "dob": "2024-07-02"},
            {"unique_id": 5, "name1": "Maurice", "name2": "Jones", "dob": "2024-07-02"},
        ]
    )

    df_3 = pd.DataFrame(
        [
            {"unique_id": 1, "name1": "John", "name2": "Smith", "dob": "2019-01-03"},
        ]
    )

    db_api = DuckDBAPI()

    n_largest_dedupe_only = n_largest_blocks(
        table_or_tables=df_1,
        blocking_rule=block_on("name1", "substr(name2,1,1)"),
        link_type="dedupe_only",
        db_api=db_api,
    ).as_pandas_dataframe()

    sql = """
    with
    a as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from df_1
    group by key_0, key_1
    ),

    b as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from df_1
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_dedupe_only = duckdb.sql(sql).df()

    pd.testing.assert_frame_equal(n_largest_dedupe_only, n_largest_manual_dedupe_only)

    n_largest_link_and_dedupe = n_largest_blocks(
        table_or_tables=[df_1, df_2],
        blocking_rule=block_on("name1", "substr(name2,1,1)"),
        link_type="link_and_dedupe",
        db_api=db_api,
    ).as_pandas_dataframe()

    sql = """
    with
    a as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from (select * from df_1 union all select * from df_2)
    group by key_0, key_1
    ),

    b as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from (select * from df_1 union all select * from df_2)
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_link_and_dedupe = duckdb.sql(sql).df()

    pd.testing.assert_frame_equal(
        n_largest_link_and_dedupe, n_largest_manual_link_and_dedupe
    )

    n_largest_link_only = n_largest_blocks(
        table_or_tables=[df_1, df_2],
        blocking_rule=block_on("name1", "substr(name2,1,1)"),
        link_type="link_only",
        db_api=db_api,
    ).as_pandas_dataframe()

    sql = """
    with
    a as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from df_1
    group by key_0, key_1
    ),

    b as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from df_2
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_link_only = duckdb.sql(sql).df()

    pd.testing.assert_frame_equal(n_largest_link_only, n_largest_manual_link_only)

    n_largest_link_only_3 = n_largest_blocks(
        table_or_tables=[df_1, df_2, df_3],
        blocking_rule=block_on("name1", "substr(name2,1,1)"),
        link_type="link_only",
        db_api=db_api,
    ).as_pandas_dataframe()

    sql = """
    with
    a as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from (select * from df_1 union all select * from df_2 union all select * from df_3)
    group by key_0, key_1
    ),

    b as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from (select * from df_1 union all select * from df_2 union all select * from df_3)
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_link_only_3 = duckdb.sql(sql).df()

    pd.testing.assert_frame_equal(n_largest_link_only_3, n_largest_manual_link_only_3)

    n_largest_link_and_dedupe_inverted = n_largest_blocks(
        table_or_tables=[df_1, df_2],
        blocking_rule="l.name1 = r.name2 and l.name2 = r.name1",
        link_type="link_and_dedupe",
        db_api=db_api,
    ).as_pandas_dataframe()

    sql = """
    with
    a as (
    select name1 as key_0, name2 as key_1, count(*) as c
    from (select * from df_1 union all select * from df_2)
    group by key_0, key_1
    ),

    b as (
    select name2 as key_0, name1 as key_1, count(*) as c
    from (select * from df_1 union all select * from df_2)
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_link_and_dedupe_inverted = duckdb.sql(sql).df()

    pd.testing.assert_frame_equal(
        n_largest_link_and_dedupe_inverted.sort_values(["key_0", "key_1"]).reset_index(
            drop=True
        ),
        n_largest_manual_link_and_dedupe_inverted.sort_values(
            ["key_0", "key_1"]
        ).reset_index(drop=True),
    )


def test_blocking_rule_parentheses_equivalence():
    """Test that different blocking rule formats produce identical results
    (issue #2501)"""

    # Test data
    data = [
        {
            "unique_id": 1,
            "forename1_std": "john",
            "forename2_std": "michael",
            "dob_std": "1990-01-15",
            "case_number": "A001",
        },
        {
            "unique_id": 2,
            "forename1_std": "john",
            "forename2_std": "michael",
            "dob_std": "1990-01-15",
            "case_number": "A002",
        },
        {
            "unique_id": 3,
            "forename1_std": "sarah",
            "forename2_std": "jane",
            "dob_std": "1985-03-22",
            "case_number": "A003",
        },
        {
            "unique_id": 4,
            "forename1_std": "robert",
            "forename2_std": "james",
            "dob_std": "1992-11-30",
            "case_number": "A004",
        },
    ]
    df = pd.DataFrame(data)
    db_api = DuckDBAPI()

    # Test three variations of the same blocking rule
    br_with_brl = brl.And(
        brl.block_on("forename1_std", "forename2_std", "dob_std"),
        brl.CustomRule("l.case_number != r.case_number"),
    )

    br_with_parens = """
    ((l.forename1_std = r.forename1_std)
    AND (l.forename2_std = r.forename2_std)
    AND (l.dob_std = r.dob_std))
    AND (l.case_number != r.case_number)
    """

    br_without_parens = """
    l.forename1_std = r.forename1_std
    AND l.forename2_std = r.forename2_std
    AND l.dob_std = r.dob_std
    AND l.case_number != r.case_number
    """

    # Get results for each variation
    result_brl = count_comparisons_from_blocking_rule(
        table_or_tables=df,
        blocking_rule=br_with_brl,
        link_type="dedupe_only",
        db_api=db_api,
    )

    result_with_parens = count_comparisons_from_blocking_rule(
        table_or_tables=df,
        blocking_rule=br_with_parens,
        link_type="dedupe_only",
        db_api=db_api,
    )

    result_without_parens = count_comparisons_from_blocking_rule(
        table_or_tables=df,
        blocking_rule=br_without_parens,
        link_type="dedupe_only",
        db_api=db_api,
    )

    # Check specific values
    for result in [result_brl, result_with_parens, result_without_parens]:
        assert result["number_of_comparisons_generated_pre_filter_conditions"] == 6
        assert result["number_of_comparisons_to_be_scored_post_filter_conditions"] == 1
