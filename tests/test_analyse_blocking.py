import pandas as pd
from splink.duckdb.duckdb_linker import DuckDBLinker
from tests.basic_settings import get_settings_dict
from splink.analyse_blocking import (
    cumulative_comparisons_generated_by_blocking_rules,
)


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


def validate_blocking_output(linker, expected_out, **kwargs):
    records = cumulative_comparisons_generated_by_blocking_rules(linker, **kwargs)

    assert expected_out["row_count"] == list(map(lambda x: x["row_count"], records))

    assert expected_out["cumulative_rows"] == list(
        map(lambda x: x["cumulative_rows"], records)
    )

    assert expected_out["cartesian"] == records[0]["cartesian"]


def test_blocking_records_accuracy():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker_settings = DuckDBLinker(df, get_settings_dict())
    linker_no_settings = DuckDBLinker([df, df])

    # dedupe only
    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [3167],
            "cumulative_rows": [3167],
            "cartesian": 499500,
        },
        blocking_rules=None,
    )

    # dedupe only with additional brs
    blocking_rules = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
    ]

    validate_blocking_output(
        linker_settings,
        expected_out={
            "row_count": [3167, 1654],
            "cumulative_rows": [3167, 4821],
            "cartesian": 499500,
        },
        blocking_rules=blocking_rules,
    )

    # link and dedupe + link only without settings
    blocking_rules = [
        "l.surname = r.surname",
        "l.first_name = r.first_name or substr(l.dob,1,4) = substr(r.dob,1,4)",
        "l.city = r.city",
    ]

    validate_blocking_output(
        linker_no_settings,
        expected_out={
            "row_count": [13591, 53472, 137280],
            "cumulative_rows": [13591, 67063, 204343],
            "cartesian": 1999000,
        },
        blocking_rules=blocking_rules,
        link_type="link_and_dedupe",
        unique_id_column_name="unique_id",
    )

    validate_blocking_output(
        linker_no_settings,
        expected_out={
            "row_count": [7257, 27190, 68640],
            "cumulative_rows": [7257, 34447, 103087],
            "cartesian": 1000000,
        },
        blocking_rules=blocking_rules,
        link_type="link_only",
        unique_id_column_name="unique_id",
    )
