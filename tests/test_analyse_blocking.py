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

    res = linker.analyse_blocking_rule("1=1")
    assert res["count_of_pairwise_comparisons_generated"] == 4 * 3 / 2

    res = linker.analyse_blocking_rule("l.first_name = r.first_name")
    assert res["count_of_pairwise_comparisons_generated"] == 1

    linker = DuckDBLinker([df_1, df_2])
    res = linker.analyse_blocking_rule("1=1", link_type="link_only")
    assert res["count_of_pairwise_comparisons_generated"] == 4 * 3

    res = linker.analyse_blocking_rule("l.surname = r.surname", link_type="link_only")
    assert res["count_of_pairwise_comparisons_generated"] == 1

    res = linker.analyse_blocking_rule(
        "l.first_name = r.first_name", link_type="link_only"
    )
    assert res["count_of_pairwise_comparisons_generated"] == 3

    res = linker.analyse_blocking_rule("1=1", link_type="link_and_dedupe")
    expected = 4 * 3 + (4 * 3 / 2) + (3 * 2 / 2)
    assert res["count_of_pairwise_comparisons_generated"] == expected

    rule = "l.first_name = r.first_name and l.surname = r.surname"
    res = linker.analyse_blocking_rule(rule, link_type="link_and_dedupe")

    assert res["count_of_pairwise_comparisons_generated"] == 1
