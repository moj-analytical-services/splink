import pandas as pd
from splink.duckdb.duckdb_linker import DuckDBLinker


def test_analyse_blocking():

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

    linker = DuckDBLinker(df_1)

    res = linker.count_num_comparisons_from_blocking_rule(
        "1=1", unique_id_column_name="unique_id"
    )
    assert res == 4 * 3 / 2

    res = linker.count_num_comparisons_from_blocking_rule(
        "l.first_name = r.first_name", unique_id_column_name="unique_id"
    )
    assert res == 1

    linker = DuckDBLinker([df_1, df_2])
    res = linker.count_num_comparisons_from_blocking_rule(
        "1=1", link_type="link_only", unique_id_column_name="unique_id"
    )
    assert res == 4 * 3

    res = linker.count_num_comparisons_from_blocking_rule(
        "l.surname = r.surname",
        link_type="link_only",
        unique_id_column_name="unique_id",
    )
    assert res == 1

    res = linker.count_num_comparisons_from_blocking_rule(
        "l.first_name = r.first_name",
        link_type="link_only",
        unique_id_column_name="unique_id",
    )
    assert res == 3

    res = linker.count_num_comparisons_from_blocking_rule(
        "1=1", link_type="link_and_dedupe", unique_id_column_name="unique_id"
    )
    expected = 4 * 3 + (4 * 3 / 2) + (3 * 2 / 2)
    assert res == expected

    rule = "l.first_name = r.first_name and l.surname = r.surname"
    res = linker.count_num_comparisons_from_blocking_rule(
        rule, link_type="link_and_dedupe", unique_id_column_name="unique_id"
    )

    assert res == 1
