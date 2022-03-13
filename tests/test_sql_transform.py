from splink.sql_transform import move_l_r_table_prefix_to_column_suffix


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
