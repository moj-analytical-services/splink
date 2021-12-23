from pyspark.sql import Row
from splink.analyse_blocking_rule import analyse_blocking_rule
from splink.default_settings import complete_settings_dict


def test_analyse_blocking_rules(spark):

    # fmt: off
    rows = [
        {"unique_id": 1, "mob": 10, "surname": "Linacre", "forename": "Robin", "source_dataset": "df1"},
        {"unique_id": 2, "mob": 10, "surname": "Linacre", "forename": "Robin", "source_dataset": "df2"},
        {"unique_id": 3, "mob": 10, "surname": "Linacer", "forename": "Robin", "source_dataset": "df1"},
        {"unique_id": 4, "mob": 7, "surname": "Smith", "forename": "John", "source_dataset": "df1"},
        {"unique_id": 5, "mob": 8, "surname": "Smith", "forename": "John", "source_dataset": "df2"},
        {"unique_id": 6, "mob": 8, "surname": "Smith", "forename": "Jon", "source_dataset": "df1"},
        {"unique_id": 7, "mob": 8, "surname": "Jones", "forename": "Robin", "source_dataset": "df2"},
    ]
    # fmt: on

    df = spark.createDataFrame(Row(**x) for x in rows)

    splink_settings = {
        "link_type": "dedupe_only",
        "comparison_columns": [
            {"col_name": "surname"},
            {"col_name": "mob"},
            {"col_name": "forename"},
        ],
        "blocking_rules": [],
    }

    splink_settings = complete_settings_dict(splink_settings, None)

    ############
    # Test 1
    ############
    blocking_rule = "l.surname = r.surname"

    results = analyse_blocking_rule(
        df, blocking_rule, splink_settings, compute_exact_comparisons=True
    )

    df.createOrReplaceTempView("df")
    sql = f"select count(*) from df as l inner join df as r on {blocking_rule}"
    expected_count = spark.sql(sql).collect()[0][0]

    expected = {
        "total_comparisons_generated_before_filter_applied": expected_count,
        "total_comparisons_generated_after_filters_applied": 4,
        "largest_group_expr": "Smith",
        "comparisons_generated_in_largest_group_before_filter_applied": 9,
        "join_strategy": "SortMergeJoin",
        "join_type": "Inner",
        "join_hashpartition_columns_left": ["surname"],
        "join_hashpartition_columns_right": ["surname"],
    }
    for key in expected.keys():
        assert results[key] == expected[key]

    ############
    # Test 2 - Cartesian
    ############
    blocking_rule = "l.surname = r.surname or l.forename = r.forename"

    results = analyse_blocking_rule(
        df, blocking_rule, splink_settings, compute_exact_comparisons=True
    )

    df.createOrReplaceTempView("df")
    sql = f"select count(*) from df as l cross join df as r"
    expected_count = spark.sql(sql).collect()[0][0]

    expected = {
        "total_comparisons_generated_before_filter_applied": expected_count,
        "total_comparisons_generated_after_filters_applied": 9,
        "largest_group_expr": None,
        "comparisons_generated_in_largest_group_before_filter_applied": None,
        "join_strategy": "Cartesian",
        "join_type": "Cartesian",
        "join_hashpartition_columns_left": [],
        "join_hashpartition_columns_right": [],
    }
    for key in expected.keys():
        assert results[key] == expected[key]

    ############
    # Test 3 - link only
    ############
    blocking_rule = "l.surname = r.surname and l.forename = r.forename"
    splink_settings = {
        "link_type": "link_only",
        "comparison_columns": [
            {"col_name": "surname"},
            {"col_name": "mob"},
            {"col_name": "forename"},
        ],
        "blocking_rules": [],
    }

    # fmt: off
    rows = [
        {"unique_id": 1, "mob": 10, "surname": "Linacre", "forename": "Robin", "source_dataset": "df1"},
        {"unique_id": 2, "mob": 10, "surname": "Linacre", "forename": "Robin", "source_dataset": "df2"},
        {"unique_id": 3, "mob": 10, "surname": "Linacre", "forename": "Robin", "source_dataset": "df1"},
        {"unique_id": 4, "mob": 10, "surname": "Linacre", "forename": "Robin", "source_dataset": "df2"},
        {"unique_id": 5, "mob": 10, "surname": "Smith", "forename": "John", "source_dataset": "df2"},

    ]
    # fmt: on

    df = spark.createDataFrame(Row(**x) for x in rows)

    splink_settings = complete_settings_dict(splink_settings, None)

    results = analyse_blocking_rule(
        df, blocking_rule, splink_settings, compute_exact_comparisons=True
    )

    df.createOrReplaceTempView("df")
    sql = f"select count(*) from df as l inner join df as r on {blocking_rule}"
    expected_count = spark.sql(sql).collect()[0][0]

    expected = {
        "total_comparisons_generated_before_filter_applied": expected_count,
        "total_comparisons_generated_after_filters_applied": 4,
        "largest_group_expr": "Linacre|Robin",
        "comparisons_generated_in_largest_group_before_filter_applied": 16,
        "join_strategy": "SortMergeJoin",
        "join_type": "Inner",
        "join_hashpartition_columns_left": ["surname", "forename"],
        "join_hashpartition_columns_right": ["surname", "forename"],
    }
    for key in expected.keys():
        assert results[key] == expected[key]