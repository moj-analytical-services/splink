from splink.input_column import InputColumn


def test_input_column():
    c = InputColumn("my_col")
    assert c.name() == '"my_col"'
    assert c.unquote().name() == "my_col"

    assert c.name_l() == '"my_col_l"'
    assert c.tf_name_l() == '"tf_my_col_l"'
    assert c.unquote().quote().l_tf_name_as_l() == '"l"."tf_my_col" as "tf_my_col_l"'
    assert c.unquote().l_tf_name_as_l() == '"l".tf_my_col as tf_my_col_l'

    c = InputColumn("SUR name")
    assert c.name() == '"SUR name"'
    assert c.name_r() == '"SUR name_r"'
    assert c.r_name_as_r() == '"r"."SUR name" as "SUR name_r"'

    c = InputColumn("col['lat']")

    name = """
    "col"['lat']
    """.strip()
    assert c.name() == name

    l_tf_name_as_l = """
    "l"."tf_col"['lat'] as "tf_col_l"['lat']
    """.strip()
    assert c.l_tf_name_as_l() == l_tf_name_as_l

    assert c.unquote().name() == "col['lat']"
    assert c.unquote().quote().name() == name

    c = InputColumn("first name", sql_dialect="spark")
    assert c.name() == "`first name`"
