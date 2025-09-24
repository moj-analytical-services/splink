import sqlglot

from splink.internals.athena.athena_helpers.athena_transforms import (
    cast_concat_as_varchar,
)
from splink.internals.input_column import InputColumn
from splink.internals.sql_transform import (
    move_l_r_table_prefix_to_column_suffix,
    sqlglot_transform_sql,
    sqlglot_tree_signature,
)


def move_l_r_test(br, expected_possibilities):
    # We don't test against a single expectation,
    # as that may depend on the underlying version of sqlglot
    # this is due to what function names sqlglot prefers
    # e.g. list_contains vs array_contains
    # ultimately we might want to refactor to stabilise this
    res = move_l_r_table_prefix_to_column_suffix(br, sqlglot_dialect="duckdb")
    assert res.lower() in expected_possibilities


def test_move_l_r_table_prefix_to_column_suffix():
    br = "l.first_name = r.first_name"
    expected = ("first_name_l = first_name_r",)
    move_l_r_test(br, expected)

    br = "substring(l.last_name, 1, 2) = substring(r.last_name, 1, 2)"
    expected = ("substring(last_name_l, 1, 2) = substring(last_name_r, 1, 2)",)
    move_l_r_test(br, expected)

    br = "l.name['first'] = r.name['first'] and levenshtein(l.dob, r.dob) < 2"
    expected = ("name_l['first'] = name_r['first'] and levenshtein(dob_l, dob_r) < 2",)
    move_l_r_test(br, expected)

    br = "concat_ws(', ', l.name, r.name)"
    expected = (
        "concat_ws(', ', name_l, name_r)",
        (
            "concat_ws(', ', coalesce(cast(name_l as text), ''), "
            "coalesce(cast(name_r as text), ''))"
        ),
    )
    move_l_r_test(br, expected)

    br = "my_custom_function(l.name, r.name)"
    expected = ("my_custom_function(name_l, name_r)",)
    move_l_r_test(br, expected)

    br = "len(list_filter(l.name_list, x -> list_contains(r.name_list, x))) >= 1"
    expected = (
        "length(list_filter(name_list_l, x -> list_contains(name_list_r, x))) >= 1",
        "length(list_filter(name_list_l, x -> array_contains(name_list_r, x))) >= 1",
    )
    move_l_r_test(br, expected)


def test_cast_concat_as_varchar():
    output = """
        select cast(l.source_dataset AS varchar) || '-__-' ||
        cast(l.unique_id AS varchar) AS concat_id
    """
    output = sqlglot.parse_one(output).sql()

    sql = "select l.source_dataset || '-__-' || l.unique_id AS concat_id"
    transformed_sql = sql = sqlglot_transform_sql(sql, cast_concat_as_varchar)
    assert transformed_sql == output

    sql = """
        select cast(l.source_dataset AS varchar) || '-__-' ||
        l.unique_id AS concat_id
    """
    transformed_sql = sql = sqlglot_transform_sql(sql, cast_concat_as_varchar)
    assert transformed_sql == output

    sql = """
        select cast(l.source_dataset AS varchar) || '-__-' ||
        cast(l.unique_id AS varchar) AS concat_id
    """
    transformed_sql = sql = sqlglot_transform_sql(sql, cast_concat_as_varchar)
    assert transformed_sql == output

    sql = "select source_dataset || '-__-' || unique_id AS concat_id"
    transformed_sql = sql = sqlglot_transform_sql(sql, cast_concat_as_varchar)
    assert transformed_sql == output.replace("l.", "")


def test_set_numeric_as_double():
    sql = "select cast('a' AS float8), cast(0.12345 AS float8)"
    transformed_sql = sqlglot.transpile(sql, write="customspark")[0]
    assert transformed_sql == "SELECT aD, 0.12345D"

    sql = "select cast('a' AS string), cast(0.12345 AS float8)"
    transformed_sql = sqlglot.transpile(sql, write="customspark")[0]
    assert transformed_sql == "SELECT CAST('a' AS STRING), 0.12345D"


def test_add_pref_and_suffix():
    dull = InputColumn("dull", sqlglot_dialect_str="duckdb")
    dull_l_r = ['"l"."dull" AS "dull_l"', '"r"."dull" AS "dull_r"']
    assert dull.l_r_names_as_l_r == dull_l_r

    assert dull.bf_name == '"bf_dull"'
    assert dull.tf_name_l == '"tf_dull_l"'
    tf_dull_l_r = ['"l"."tf_dull" AS "tf_dull_l"', '"r"."tf_dull" AS "tf_dull_r"']
    assert dull.l_r_tf_names_as_l_r == tf_dull_l_r

    ll = InputColumn("lat['long']", sqlglot_dialect_str="duckdb")
    assert ll.name_l == "\"lat_l\"['long']"

    ll_tf_l_r = [
        '"l"."tf_lat"[\'long\'] AS "tf_lat_l[\'long\']"',
        '"r"."tf_lat"[\'long\'] AS "tf_lat_r[\'long\']"',
    ]

    assert ll.l_r_tf_names_as_l_r == ll_tf_l_r

    group = InputColumn("cluster", sqlglot_dialect_str="duckdb")
    assert group.name_l == '"cluster_l"'
    assert group.bf_name == '"bf_cluster"'
    group_l_r_names = ['"l"."cluster" AS "cluster_l"', '"r"."cluster" AS "cluster_r"']
    assert group.l_r_names_as_l_r == group_l_r_names

    group_tf_l_r = [
        '"l"."tf_cluster" AS "tf_cluster_l"',
        '"r"."tf_cluster" AS "tf_cluster_r"',
    ]
    assert group.l_r_tf_names_as_l_r == group_tf_l_r

    cols = ["unique_id", "SUR name", "cluster"]
    out_cols = ['"unique_id"', '"SUR name"', '"cluster"']
    cols_class = [InputColumn(c, sqlglot_dialect_str="duckdb") for c in cols]
    assert [c.name for c in cols_class] == out_cols


def test_sqlglot_tree_signature():
    # just to make the tests a bit leaner
    # we don't care about actual signature, only how it compares to reference values
    def assert_signatures_match(sql_string: str, equivalent_sql: str) -> None:
        sig = sqlglot_tree_signature(sqlglot.parse_one(sql_string))
        expected_sig = sqlglot_tree_signature(sqlglot.parse_one(equivalent_sql))
        assert sig == expected_sig

    # the sort of expressions we allow for input columns
    assert_signatures_match("col_1", "col_2")
    assert_signatures_match("col_1[1]", "col_2[10]")
    assert_signatures_match("col_1['lat']", "another_col['key']")
    # more nested expressions
    assert_signatures_match("name_l = name_r", "col_l = col_r")
    assert_signatures_match(
        "name_l IS NULL",
        "col IS NULL",
    )
    assert_signatures_match(
        "name_l IS NULL OR name_r IS NULL",
        "some_col IS NULL OR another_col IS NULL",
    )
    assert_signatures_match(
        "name_l[1] = name_r[1]",
        "col[5] = another_col[5]",
    )
    assert_signatures_match(
        "name_l['first'] = name_r['first']",
        "address_l['primary'] = address_r['secondary']",
    )
