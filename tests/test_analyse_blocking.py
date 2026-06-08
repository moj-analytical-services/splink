import pyarrow as pa
import pytest

import splink.blocking_rule_library as brl
from splink.blocking_analysis import (
    chart_comparisons_from_blocking_rules,
    count_comparisons_from_blocking_rules,
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

    df_1 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smith"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
        {"unique_id": 4, "first_name": "John", "surname": "Brown"},
    ]

    df_2 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jayne", "surname": "Tailor"},
    ]

    df_3 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smith"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
    ]

    db_api = helper.db_api()
    df1_sdf = db_api.register(df_1)
    df2_sdf = db_api.register(df_2)
    df3_sdf = db_api.register(df_3)

    args = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "unique_id",
    }

    res = count_comparisons_from_blocking_rules(df1_sdf, blocking_rules="1=1", **args)[
        0
    ]["marginal_comparison_count"]
    assert res == 4 * 3 / 2

    res = count_comparisons_from_blocking_rules(
        df1_sdf, blocking_rules=block_on("first_name"), **args
    )[0]["marginal_comparison_count"]
    assert res == 1

    args["link_type"] = "link_only"
    res = count_comparisons_from_blocking_rules(
        [df1_sdf, df2_sdf], blocking_rules="1=1", **args
    )[0]["marginal_comparison_count"]
    assert res == 4 * 3

    res = count_comparisons_from_blocking_rules(
        [df1_sdf, df2_sdf], blocking_rules=block_on("surname"), **args
    )[0]["marginal_comparison_count"]
    assert res == 1

    res = count_comparisons_from_blocking_rules(
        [df1_sdf, df2_sdf],
        blocking_rules=block_on("first_name"),
        **args,
    )[0]["marginal_comparison_count"]
    assert res == 3

    res = count_comparisons_from_blocking_rules(
        [df1_sdf, df2_sdf, df3_sdf], blocking_rules="1=1", **args
    )[0]["marginal_comparison_count"]
    assert res == 4 * 3 + 4 * 2 + 2 * 3

    args["link_type"] = "link_and_dedupe"
    res = count_comparisons_from_blocking_rules(
        [df1_sdf, df2_sdf], blocking_rules="1=1", **args
    )[0]["marginal_comparison_count"]
    expected = 4 * 3 + (4 * 3 / 2) + (3 * 2 / 2)
    assert res == expected

    rule = "l.first_name = r.first_name and l.surname = r.surname"
    res = count_comparisons_from_blocking_rules(
        [df1_sdf, df2_sdf], blocking_rules=rule, **args
    )[0]["marginal_comparison_count"]
    assert res == 1

    rule = block_on("first_name", "surname")
    res = count_comparisons_from_blocking_rules(
        [df1_sdf, df2_sdf], blocking_rules=rule, **args
    )[0]["marginal_comparison_count"]
    assert res == 1


@mark_with_dialects_including("duckdb", "spark", pass_dialect=True)
def test_blocking_analysis_slow_methodology_exploding(test_helpers, dialect):
    helper = test_helpers[dialect]

    df_1 = [
        {"unique_id": 1, "first_name": "John", "postcode": [1001, 1002]},
        {"unique_id": 2, "first_name": "Mary", "postcode": [1002, 1003]},
        {"unique_id": 3, "first_name": "Jane", "postcode": [1003]},
        {"unique_id": 4, "first_name": "John", "postcode": [1001]},
    ]

    df_2 = [
        {"unique_id": 1, "first_name": "John", "postcode": [1001, 1004]},
        {"unique_id": 2, "first_name": "Mary", "postcode": [1003, 1004]},
        {"unique_id": 3, "first_name": "Jayne", "postcode": [1003]},
    ]

    db_api = helper.db_api()
    df_1_sdf = db_api.register(df_1)
    df_2_sdf = db_api.register(df_2)

    args = {
        "link_type": "link_only",
        "unique_id_column_name": "unique_id",
    }

    rule = block_on("postcode", arrays_to_explode=["postcode"])
    res = count_comparisons_from_blocking_rules(
        [df_1_sdf, df_2_sdf], blocking_rules=rule, **args
    )[0]["marginal_comparison_count"]
    assert res == 6

    args = {
        "link_type": "link_and_dedupe",
        "unique_id_column_name": "unique_id",
    }

    rule = block_on("postcode", arrays_to_explode=["postcode"])
    res = count_comparisons_from_blocking_rules(
        [df_1_sdf, df_2_sdf], blocking_rules=rule, **args
    )[0]["marginal_comparison_count"]
    assert res == 3 + 6 + 2


# Just run in duckdb for speed
@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_blocking_analysis_slow_methodology_exploding_2(test_helpers, dialect):
    helper = test_helpers[dialect]

    db_api = helper.db_api()

    cols = ("unique_id", "sds", "first_name", "postcode", "age", "amount")

    rows_1 = [
        (1, "a", "John", [1, 2], [2, 3], 5),
        (2, "a", "Mary", [10, 11, 12, 13], [11, 12], 5),
    ]
    df_1 = [{col: datum for col, datum in zip(cols, row)} for row in rows_1]

    rows_2 = [
        (1, "b", "John", [1, 4], [1, 2, 3], 5),
        (2, "b", "John", [5], [1, 2, 3], 5),
        (3, "b", "John", [1], [1], 5),
        (4, "b", "John", [1], [3], 1),
        (5, "b", "Mary", [10], [11, 12], 5),
        (6, "b", "Mary", [10], [11, 12], 1),
        (7, "b", "Mary", [10, 11, 12, 13], [11, 12], 1),
    ]
    df_2 = [{col: datum for col, datum in zip(cols, row)} for row in rows_2]

    df_1_sdf = db_api.register(df_1)
    df_2_sdf = db_api.register(df_2)

    args = {
        "link_type": "link_only",
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

    res = count_comparisons_from_blocking_rules(
        [df_1_sdf, df_2_sdf], blocking_rules=rule, **args
    )[0]["marginal_comparison_count"]

    sql = f"""
    select count(*) as count
    from {df_1_sdf.physical_name} as l
    cross join {df_2_sdf.physical_name} as r
    where
    l.first_name = r.first_name
    and len(array_intersect(l.postcode, r.postcode)) > 0
    and len(array_intersect(l.age, r.age)) > 0
    and r.amount > 2
    """

    c = db_api.duckdb_con.sql(sql).fetchone()[0]

    assert res == c


def validate_blocking_output(comparison_count_args, expected_out):
    records = count_comparisons_from_blocking_rules(**comparison_count_args)

    assert expected_out["marginal_comparison_count"] == [
        r["marginal_comparison_count"] for r in records
    ]

    assert expected_out["cumulative_comparison_count"] == list(
        map(lambda x: x["cumulative_comparison_count"], records)
    )

    assert (
        expected_out["total_possible_comparison_count"]
        == records[0]["total_possible_comparison_count"]
    )


@mark_with_dialects_excluding()
def test_source_dataset_works_as_expected(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    data_1 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smith"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
        {"unique_id": 4, "first_name": "John", "surname": "Brown"},
    ]

    data_2 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jayne", "surname": "Tailor"},
    ]

    data_concat = [{**row, "src_dataset": "df_1"} for row in data_1] + [
        {**row, "src_dataset": "df_2"} for row in data_2
    ]

    db_api = helper.db_api()
    df_concat_sdf = db_api.register(data_concat)
    df_1_sdf = db_api.register(data_1)
    df_2_sdf = db_api.register(data_2)

    r1 = count_comparisons_from_blocking_rules(
        df_concat_sdf,
        blocking_rules=[block_on("first_name")],
        unique_id_column_name="unique_id",
        source_dataset_column_name="src_dataset",
        link_type="link_only",
    )

    r2 = count_comparisons_from_blocking_rules(
        [df_1_sdf, df_2_sdf],
        blocking_rules=[block_on("first_name")],
        unique_id_column_name="unique_id",
        link_type="link_only",
        source_dataset_column_name="source_dataset",
    )
    # The descriptive fields reference the source dataset column name, which
    # differs between the two calls, so compare the computed counts only.
    assert [r["marginal_comparison_count"] for r in r1] == [
        r["marginal_comparison_count"] for r in r2
    ]
    assert [r["cumulative_comparison_count"] for r in r1] == [
        r["cumulative_comparison_count"] for r in r2
    ]
    assert (
        r1[0]["total_possible_comparison_count"]
        == r2[0]["total_possible_comparison_count"]
    )

    # split table into 3 with alternating rows
    df_1 = fake_1000.take(list(range(0, 1000, 3)))
    df_2 = fake_1000.take(list(range(1, 1000, 3)))
    df_3 = fake_1000.take(list(range(2, 1000, 3)))

    df_1_no_sds_sdf = db_api.register(df_1)
    df_2_no_sds_sdf = db_api.register(df_2)
    df_3_no_sds_sdf = db_api.register(df_3)

    df_concat_2 = pa.concat_tables(
        [
            df_1.append_column("sds", pa.repeat("df_1_name", df_1.num_rows)),
            df_2.append_column("sds", pa.repeat("df_2_name", df_2.num_rows)),
        ]
    )
    df_concat_3 = pa.concat_tables(
        [
            df_1.append_column("sds", pa.repeat("df_1_name", df_1.num_rows)),
            df_2.append_column("sds", pa.repeat("df_2_name", df_2.num_rows)),
            df_3.append_column("sds", pa.repeat("df_3_name", df_3.num_rows)),
        ]
    )

    df_concat_2_sdf = db_api.register(df_concat_2)
    df_concat_3_sdf = db_api.register(df_concat_3)

    r1 = count_comparisons_from_blocking_rules(
        df_concat_3_sdf,
        blocking_rules=block_on("first_name"),
        link_type="link_only",
        unique_id_column_name="unique_id",
        source_dataset_column_name="sds",
    )

    r2 = count_comparisons_from_blocking_rules(
        [df_1_no_sds_sdf, df_2_no_sds_sdf, df_3_no_sds_sdf],
        blocking_rules=block_on("first_name"),
        link_type="link_only",
        unique_id_column_name="unique_id",
    )
    # Both of the above use the vertical concat of the two datasets so should
    # be equivalent
    assert r1[0]["marginal_comparison_count"] == r2[0]["marginal_comparison_count"]

    r1 = count_comparisons_from_blocking_rules(
        df_concat_2_sdf,
        blocking_rules=block_on("first_name"),
        link_type="link_only",
        unique_id_column_name="unique_id",
        source_dataset_column_name="sds",
    )

    r2 = count_comparisons_from_blocking_rules(
        [df_1_no_sds_sdf, df_2_no_sds_sdf],
        blocking_rules=block_on("first_name"),
        link_type="link_only",
        unique_id_column_name="unique_id",
    )
    # After filters, the number of comparisons should be the same
    assert r1[0]["marginal_comparison_count"] == r2[0]["marginal_comparison_count"]


@mark_with_dialects_excluding()
def test_blocking_records_accuracy(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.db_api()

    # resolve an issue w/ pyspark nulls

    data = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
        {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
        {"unique_id": 3, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
        {"unique_id": 4, "first_name": "Kim", "surname": "Lee", "dob": None},
    ]
    df_sdf = db_api.register(data)

    comparison_count_args = {
        "splink_dataframe_or_dataframes": df_sdf,
        "blocking_rules": [block_on("first_name")],
        "link_type": "dedupe_only",
        "unique_id_column_name": "unique_id",
    }

    n = len(data)
    # dedupe only
    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "marginal_comparison_count": [1],
            "cumulative_comparison_count": [1],
            "total_possible_comparison_count": n * (n - 1) / 2,
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
            "marginal_comparison_count": [1, 1],
            "cumulative_comparison_count": [1, 2],
            "total_possible_comparison_count": n * (n - 1) / 2,
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
            "marginal_comparison_count": [1, 0, 1],
            "cumulative_comparison_count": [1, 1, 2],
            "total_possible_comparison_count": n * (n - 1) / 2,
        },
    )

    # link and dedupe + link only
    comparison_count_args["source_dataset_column_name"] = "source_dataset"

    data_l = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
        {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
    ]

    data_r = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
        {"unique_id": 2, "first_name": "Kim", "surname": "Lee", "dob": None},
    ]

    df_l_sdf = db_api.register(data_l)
    df_r_sdf = db_api.register(data_r)

    blocking_rules = [
        "l.surname = r.surname",  # 2l:2r,
        Or(
            block_on("first_name"), CustomRule("substr(l.dob,1,4) = substr(r.dob,1,4)")
        ),  # 1r:1r, 1l:2l, 1l:2r
        "l.surname = r.surname",
    ]

    comparison_count_args = {
        "splink_dataframe_or_dataframes": [df_l_sdf, df_r_sdf],
        "link_type": "link_and_dedupe",
        "unique_id_column_name": "unique_id",
        "blocking_rules": blocking_rules,
        "source_dataset_column_name": "source_dataset",
    }

    validate_blocking_output(
        comparison_count_args,
        expected_out={
            "marginal_comparison_count": [1, 3, 0],
            "cumulative_comparison_count": [1, 4, 4],
            "total_possible_comparison_count": 1 + 1 + 4,  # within, within, between
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
            "marginal_comparison_count": [1, 2, 0],
            "cumulative_comparison_count": [1, 3, 3],
            "total_possible_comparison_count": 4,
        },
    )

    # link and dedupe
    data_1 = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
        {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
    ]

    data_2 = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
        {"unique_id": 2, "first_name": "Kim", "surname": "Lee", "dob": None},
    ]
    data_3 = [
        {"unique_id": 1, "first_name": "Tom", "surname": "Ray", "dob": "1980-03-22"},
    ]

    df_1_sdf = db_api.register(data_1)
    df_2_sdf = db_api.register(data_2)
    df_3_sdf = db_api.register(data_3)

    comparison_count_args = {
        "splink_dataframe_or_dataframes": [df_1_sdf, df_2_sdf, df_3_sdf],
        "link_type": "link_and_dedupe",
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
            "marginal_comparison_count": [2, 2],
            "cumulative_comparison_count": [2, 4],
            "total_possible_comparison_count": 5 * 4 / 2,
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
            "marginal_comparison_count": [2, 2],
            "cumulative_comparison_count": [2, 4],
            "total_possible_comparison_count": 8,
        },
    )


def test_blocking_rule_accepts_different_dialects():
    br = "l.first_name = r.first_name"
    br = BlockingRule(br, sql_dialect_str="spark")
    assert br._equi_join_conditions == [("first_name", "first_name")]

    br = "l.`hi THERE` = r.`hi THERE`"
    br = BlockingRule(br, sql_dialect_str="spark")

    assert br._equi_join_conditions == [("`hi THERE`", "`hi THERE`")]


@mark_with_dialects_excluding()
def test_chart(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    db_api = helper.db_api()

    df_sdf = db_api.register(fake_1000)
    chart_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=[block_on("first_name"), "l.surname = r.surname"],
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
    )


@mark_with_dialects_excluding()
def test_n_largest_blocks(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.db_api()

    data_1 = [
        {"unique_id": 1, "name1": "Mary", "name2": "Jones", "dob": "2024-07-02"},
        {"unique_id": 2, "name1": "Mary", "name2": "Jones", "dob": "2024-07-02"},
        {"unique_id": 3, "name1": "Mary", "name2": "Jones", "dob": "2024-11-28"},
        {"unique_id": 4, "name1": "Maurice", "name2": "Jones", "dob": "2024-07-02"},
        {"unique_id": 5, "name1": "Jones", "name2": "Maurice", "dob": "2024-07-02"},
        {"unique_id": 6, "name1": "Jones", "name2": "Maurice", "dob": "2024-07-02"},
    ]

    data_2 = [
        {"unique_id": 1, "name1": "Mary", "name2": "Jones", "dob": "2024-07-02"},
        {"unique_id": 2, "name1": "Mary", "name2": "Jones", "dob": "2024-07-02"},
        {"unique_id": 3, "name1": "Mary", "name2": "Jones", "dob": "2024-11-28"},
        {"unique_id": 4, "name1": "Jones", "name2": "Maurice", "dob": "2024-07-02"},
        {"unique_id": 5, "name1": "Maurice", "name2": "Jones", "dob": "2024-07-02"},
    ]

    data_3 = [
        {"unique_id": 1, "name1": "John", "name2": "Smith", "dob": "2019-01-03"},
    ]

    df_1_sdf = db_api.register(data_1)
    df_2_sdf = db_api.register(data_2)
    df_3_sdf = db_api.register(data_3)

    n_largest_dedupe_only = n_largest_blocks(
        df_1_sdf,
        blocking_rule=block_on("name1", "substr(name2,1,1)"),
        link_type="dedupe_only",
    )

    sql = f"""
    with
    a as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from {df_1_sdf.physical_name}
    group by key_0, key_1
    ),

    b as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from {df_1_sdf.physical_name}
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_dedupe_only = db_api.query_sql(sql)

    assert n_largest_dedupe_only.as_dict() == n_largest_manual_dedupe_only.as_dict()

    n_largest_link_and_dedupe = n_largest_blocks(
        [df_1_sdf, df_2_sdf],
        blocking_rule=block_on("name1", "substr(name2,1,1)"),
        link_type="link_and_dedupe",
    )

    sql = f"""
    with
    a as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from (
        select * from {df_1_sdf.physical_name}
        union all
        select * from {df_2_sdf.physical_name}
    )
    group by key_0, key_1
    ),

    b as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from (
        select * from {df_1_sdf.physical_name}
        union all
        select * from {df_2_sdf.physical_name}
    )
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_link_and_dedupe = db_api.query_sql(sql)

    assert (
        n_largest_link_and_dedupe.as_dict()
        == n_largest_manual_link_and_dedupe.as_dict()
    )

    n_largest_link_only = n_largest_blocks(
        [df_1_sdf, df_2_sdf],
        blocking_rule=block_on("name1", "substr(name2,1,1)"),
        link_type="link_only",
    )

    sql = f"""
    with
    a as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from {df_1_sdf.physical_name}
    group by key_0, key_1
    ),

    b as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from {df_2_sdf.physical_name}
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_link_only = db_api.query_sql(sql)

    assert n_largest_link_only.as_dict() == n_largest_manual_link_only.as_dict()

    n_largest_link_only_3 = n_largest_blocks(
        [df_1_sdf, df_2_sdf, df_3_sdf],
        blocking_rule=block_on("name1", "substr(name2,1,1)"),
        link_type="link_only",
    )

    sql = f"""
    with
    a as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from (
        select * from {df_1_sdf.physical_name}
        union all
        select * from {df_2_sdf.physical_name}
        union all
        select * from {df_3_sdf.physical_name}
    )
    group by key_0, key_1
    ),

    b as (
    select name1 as key_0, substr(name2,1,1) as key_1, count(*) as c
    from (
        select * from {df_1_sdf.physical_name}
        union all
        select * from {df_2_sdf.physical_name}
        union all
        select * from {df_3_sdf.physical_name}
    )
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_link_only_3 = db_api.query_sql(sql)

    assert n_largest_link_only_3.as_dict() == n_largest_manual_link_only_3.as_dict()

    n_largest_link_and_dedupe_inverted = n_largest_blocks(
        [df_1_sdf, df_2_sdf],
        blocking_rule="l.name1 = r.name2 and l.name2 = r.name1",
        link_type="link_and_dedupe",
    )

    sql = f"""
    with
    a as (
    select name1 as key_0, name2 as key_1, count(*) as c
    from (
        select * from {df_1_sdf.physical_name}
        union all
        select * from {df_2_sdf.physical_name}
    )
    group by key_0, key_1
    ),

    b as (
    select name2 as key_0, name1 as key_1, count(*) as c
    from (
        select * from {df_1_sdf.physical_name}
        union all
        select * from {df_2_sdf.physical_name}
    )
    group by key_0, key_1)

    select a.key_0, a.key_1, a.c as count_l, b.c as count_r, a.c * b.c as block_count
    from a inner join b
    on a.key_0 = b.key_0 and a.key_1 = b.key_1
    order by block_count desc
    """
    n_largest_manual_link_and_dedupe_inverted = db_api.query_sql(sql)

    # ordering irrelevant, but must be unambiguous and consistent
    order_sql = "SELECT * FROM {this} ORDER BY key_0"
    assert (
        n_largest_link_and_dedupe_inverted.query_sql(order_sql).as_dict()
        == n_largest_manual_link_and_dedupe_inverted.query_sql(order_sql).as_dict()
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
    db_api = DuckDBAPI()

    df_sdf = db_api.register(data)

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
    result_brl = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=br_with_brl,
        link_type="dedupe_only",
    )

    result_with_parens = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=br_with_parens,
        link_type="dedupe_only",
    )

    result_without_parens = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=br_without_parens,
        link_type="dedupe_only",
    )

    # Check specific values
    for result in [result_brl, result_with_parens, result_without_parens]:
        assert result[0]["marginal_comparison_count"] == 1


def _wide_block_df(n_per_group=120):
    # A dataset with a small number of distinct values in `grp` so that blocking
    # on `grp` generates a large number of comparisons.  This makes the sampling
    # based estimate stable enough to test.
    rows = []
    uid = 0
    for grp in ["A", "B", "C"]:
        for _ in range(n_per_group):
            uid += 1
            rows.append({"unique_id": uid, "grp": grp})
    return rows


def test_count_comparisons_estimate_mode():
    db_api = DuckDBAPI()
    df_sdf = db_api.register(_wide_block_df())

    default = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=block_on("grp"),
        link_type="dedupe_only",
    )[0]
    exact = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=block_on("grp"),
        link_type="dedupe_only",
        record_sample_proportion=1.0,
    )[0]
    estimate = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=block_on("grp"),
        link_type="dedupe_only",
        record_sample_proportion=0.3,
    )[0]

    # Metadata signalling the approximation
    assert default["record_sample_proportion"] == 0.05
    assert default["is_estimate"] is True
    assert exact["record_sample_proportion"] == 1.0
    assert exact["is_estimate"] is False
    assert estimate["record_sample_proportion"] == 0.3
    assert estimate["is_estimate"] is True

    # The estimated count is numeric and in the right ballpark
    exact_post = exact["marginal_comparison_count"]
    est_post = estimate["marginal_comparison_count"]
    assert isinstance(est_post, (int, float))
    assert 0.5 * exact_post <= est_post <= 2.0 * exact_post


def test_count_comparisons_warns_when_sampled_pairs_are_low():
    db_api = DuckDBAPI()
    df_sdf = db_api.register(_wide_block_df(n_per_group=40))

    with pytest.warns(
        UserWarning,
        match="below the recommended minimum of 1,000",
    ):
        count_comparisons_from_blocking_rules(
            df_sdf,
            blocking_rules=block_on("grp"),
            link_type="dedupe_only",
            record_sample_proportion=0.1,
        )


def test_cumulative_data_estimate_mode():
    db_api = DuckDBAPI()
    df_sdf = db_api.register(_wide_block_df())

    blocking_rules = [block_on("grp")]

    default = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=blocking_rules,
        link_type="dedupe_only",
    )
    exact = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=blocking_rules,
        link_type="dedupe_only",
        record_sample_proportion=1.0,
    )
    estimate = count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=blocking_rules,
        link_type="dedupe_only",
        record_sample_proportion=0.3,
    )

    assert default[0]["record_sample_proportion"] == 0.05
    assert default[0]["is_estimate"] is True
    assert exact[0]["record_sample_proportion"] == 1.0
    assert exact[0]["is_estimate"] is False
    assert estimate[0]["record_sample_proportion"] == 0.3
    assert estimate[0]["is_estimate"] is True
    # The total possible comparison count is unaffected by sampling.
    assert (
        estimate[0]["total_possible_comparison_count"]
        == exact[0]["total_possible_comparison_count"]
    )

    exact_rows = exact[0]["marginal_comparison_count"]
    est_rows = estimate[0]["marginal_comparison_count"]
    assert 0.5 * exact_rows <= est_rows <= 2.0 * exact_rows
    # Cumulative count for a single rule equals the marginal count.
    assert estimate[0]["cumulative_comparison_count"] == est_rows


def test_linker_blocking_analysis_uses_settings_defaults(fake_1000):
    from splink.internals.linker import Linker

    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            block_on("first_name"),
            block_on("surname"),
        ],
        "comparisons": [],
    }
    linker = Linker(df_sdf, settings)

    # count_comparisons_from_blocking_rules via the linker
    res = linker.blocking_analysis.count_comparisons_from_blocking_rules(
        [block_on("first_name")]
    )
    assert res[0]["record_sample_proportion"] == 0.05
    assert res[0]["is_estimate"] is True
    assert res[0]["marginal_comparison_count"] > 0

    # Defaulting blocking_rules to those in the settings
    records = linker.blocking_analysis.count_comparisons_from_blocking_rules()
    assert len(records) == 2

    # Chart, defaulting blocking_rules to those in the settings
    chart = linker.blocking_analysis.chart_comparisons_from_blocking_rules()
    assert chart.chart_dict["title"]["text"] == (
        "Estimated Count of Additional Comparisons Generated by Each Blocking "
        "Rule from 5% sample"
    )

    # Estimate mode is reachable via the linker too
    with pytest.warns(
        UserWarning,
        match="below the recommended minimum of 1,000",
    ):
        est = linker.blocking_analysis.count_comparisons_from_blocking_rules(
            [block_on("first_name")],
            record_sample_proportion=0.5,
        )
    assert est[0]["record_sample_proportion"] == 0.5
    assert est[0]["is_estimate"] is True

    # n_largest_blocks via the linker
    n_largest = linker.blocking_analysis.n_largest_blocks(
        block_on("first_name"), n_largest=3
    ).as_record_dict()
    assert len(n_largest) <= 3
