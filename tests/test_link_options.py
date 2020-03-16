import pytest
import sqlite3
import pandas as pd

from splink.blocking import _sql_gen_block_using_rules, _sql_gen_vertically_concatenate, _get_columns_to_retain_blocking

from splink.settings import complete_settings_dict


def test_link_only(link_dedupe_data):

    settings = {
        "link_type": "link_only",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}],
        "blocking_rules": [
            "l.first_name = r.first_name",
            "l.surname = r.surname"
        ]
    }
    settings = complete_settings_dict(settings, spark=None)
    ctr = _get_columns_to_retain_blocking(settings)
    sql = _sql_gen_block_using_rules("link_only", ctr, settings["blocking_rules"])
    df  = pd.read_sql(sql, link_dedupe_data)
    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1,1,2,2]
    assert list(df["unique_id_r"]) == [7,9,8,9]


def test_link_dedupe(link_dedupe_data):

    settings = {
        "link_type": "link_and_dedupe",
        "comparison_columns": [{"col_name": "first_name"},
                            {"col_name": "surname"}],
        "blocking_rules": [
            "l.first_name = r.first_name",
            "l.surname = r.surname"
        ]
    }
    settings = complete_settings_dict(settings, spark=None)
    ctr = _get_columns_to_retain_blocking(settings)
    sql = _sql_gen_block_using_rules("link_and_dedupe", ctr, settings["blocking_rules"])
    df  = pd.read_sql(sql, link_dedupe_data)
    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1,1,2,2,7,8]
    assert list(df["unique_id_r"]) == [7,9,8,9,9,9]
