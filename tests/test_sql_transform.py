import sqlglot
from splink.sql_transform import (
    move_l_r_table_prefix_to_column_suffix,
    cast_concat_as_varchar,
)
from splink.spark.custom_spark_dialect import Dialect


def test_move_l_r_table_prefix_to_column_suffix():

    br = "l.first_name = r.first_name"
    res = move_l_r_table_prefix_to_column_suffix(br)
    expected = "first_name_l = first_name_r"
    assert res.lower() == expected.lower()

    br = "substr(l.last_name, 1, 2) = substr(r.last_name, 1, 2)"
    res = move_l_r_table_prefix_to_column_suffix(br)
    expected = "substr(last_name_l, 1, 2) = substr(last_name_r, 1, 2)"
    assert res.lower() == expected.lower()

    br = "l.name['first'] = r.name['first'] and levenshtein(l.dob, r.dob) < 2"
    res = move_l_r_table_prefix_to_column_suffix(br)
    expected = "name_l['first'] = name_r['first'] and levenshtein(dob_l, dob_r) < 2"
    assert res.lower() == expected.lower()


def test_cast_concat_as_varchar():

    output = """
        select cast(l.source_dataset as varchar) || '-__-' ||
        cast(l.unique_id as varchar) as concat_id
    """
    output = sqlglot.parse_one(output).sql()

    sql = "select l.source_dataset || '-__-' || l.unique_id as concat_id"
    transformed_sql = cast_concat_as_varchar(sql)
    assert transformed_sql == output

    sql = """
        select cast(l.source_dataset as varchar) || '-__-' ||
        l.unique_id as concat_id
    """
    transformed_sql = cast_concat_as_varchar(sql)
    assert transformed_sql == output

    sql = """
        select cast(l.source_dataset as varchar) || '-__-' ||
        cast(l.unique_id as varchar) as concat_id
    """
    transformed_sql = cast_concat_as_varchar(sql)
    assert transformed_sql == output

    sql = "select source_dataset || '-__-' || unique_id as concat_id"
    transformed_sql = cast_concat_as_varchar(sql)
    assert transformed_sql == output.replace("l.", "")


def test_set_numeric_as_double():
    sql = "select cast('a' as double), cast(0.12345 as double)"
    transformed_sql = sqlglot.transpile(sql, write="customspark")[0]
    assert transformed_sql == "select 'a'D, 0.12345D"

    sql = "select cast('a' as string), cast(0.12345 as double)"
    transformed_sql = sqlglot.transpile(sql, write="customspark")[0]
    assert transformed_sql == "select cast('a' as string), 0.12345D"
