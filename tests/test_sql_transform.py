import sqlglot
from splink.sql_transform import (
    move_l_r_table_prefix_to_column_suffix,
    cast_concat_as_varchar,
)
from splink.spark.custom_spark_dialect import Dialect  # noqa 401
from splink.input_column import InputColumn


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
    assert transformed_sql == "SELECT aD, 0.12345D"

    sql = "select cast('a' as string), cast(0.12345 as double)"
    transformed_sql = sqlglot.transpile(sql, write="customspark")[0]
    assert transformed_sql == "SELECT CAST('a' AS STRING), 0.12345D"


def test_add_pref_and_suffix():
    dull = InputColumn("dull")
    dull_l_r = ["l.dull as dull_l", "r.dull as dull_r"]
    assert dull.l_r_names_as_l_r() == dull_l_r

    assert dull.bf_name() == "bf_dull"
    dull._has_tf_adjustments = True
    assert dull.tf_name_l() == "tf_dull_l"
    tf_dull_l_r = ["l.tf_dull as tf_dull_l", "r.tf_dull as tf_dull_r"]
    assert dull.l_r_tf_names_as_l_r() == tf_dull_l_r

    ll = InputColumn("lat['long']")
    assert ll.name_l() == "lat_l['long']"
    ll._has_tf_adjustments = True
    ll_tf_l_r = [
        "l.tf_lat['long'] as tf_lat_l['long']",
        "r.tf_lat['long'] as tf_lat_r['long']",
    ]
    assert ll.l_r_tf_names_as_l_r() == ll_tf_l_r

    group = InputColumn("group")
    assert group.name_l() == '"group_l"'
    assert group.bf_name() == '"bf_group"'
    group_l_r_names = ['l."group" as "group_l"', 'r."group" as "group_r"']
    assert group.l_r_names_as_l_r() == group_l_r_names
    group._has_tf_adjustments = True
    group_tf_l_r = ['l."tf_group" as "tf_group_l"', 'r."tf_group" as "tf_group_r"']
    assert group.l_r_tf_names_as_l_r() == group_tf_l_r

    cols = ["unique_id", "SUR name", "group"]
    out_cols = ["unique_id", '"SUR name"', '"group"']
    cols_class = [InputColumn(c) for c in cols]
    assert [c.name() for c in cols_class] == out_cols
    assert [InputColumn(c).name(escape=False) for c in cols] == cols
