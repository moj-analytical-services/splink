from pyspark.sql import Row
from splink.case_statements import sql_gen_case_smnt_strict_equality_2
from splink.m_from_labels import estimate_m_from_labels

import pytest


def test_m_from_labels(spark):

    # fmt: off
    df_rows = [
        {"uid": "0", "sds": "df1", "first_name": "Robin", "dob": "1909-10-11"},
        {"uid": "1", "sds": "df1", "first_name": "Robin", "dob": "1909-10-11"},
        {"uid": "2", "sds": "df1", "first_name": "Robim", "dob": "1909-10-11"},
        {"uid": "3", "sds": "df1", "first_name": "James", "dob": "1909-10-10"},
    ]

    labels_rows = [
        {"uid_l": "1", "sds_l": "df1", "uid_r": "0", "sds_r": "df1"},
        {"uid_l": "2", "sds_l": "df1", "uid_r": "0", "sds_r": "df1"},
        {"uid_l": "0", "sds_l": "df1", "uid_r": "3", "sds_r": "df1"},
    ]
    # fmt: on

    df = spark.createDataFrame(Row(**x) for x in df_rows)

    df_labels = spark.createDataFrame(Row(**x) for x in labels_rows)

    sql_name = """
    case
    when first_name_l = first_name_r then 2
    when substr(first_name_l, 1,3) = substr(first_name_r, 1,3) then 1
    else 0
    end
    """

    settings = {
        "comparison_columns": [
            {"col_name": "first_name", "case_expression": sql_name, "num_levels": 3},
            {
                "col_name": "dob",
                "case_expression": sql_gen_case_smnt_strict_equality_2("dob"),
            },
        ],
        "link_type": "dedupe_only",
        "unique_id_column_name": "uid",
        "source_dataset_column_name": "sds",
    }

    # This test requires graphframes and connected components, which aren't dev dependencies
    # I have checked and they pass
    # set_cc = estimate_m_from_labels(
    #     settings, df, df_labels, use_connected_components=True
    # )

    # m_first_name = set_cc["comparison_columns"][0]["m_probabilities"]

    # assert pytest.approx(m_first_name) == [3 / 6, 2 / 6, 1 / 6]

    # m_dob = set_cc["comparison_columns"][1]["m_probabilities"]
    # assert pytest.approx(m_dob) == [3 / 6, 3 / 6]

    set_nocc = estimate_m_from_labels(
        settings, df, df_labels, use_connected_components=False
    )
    m_first_name = set_nocc["comparison_columns"][0]["m_probabilities"]
    assert pytest.approx(m_first_name) == [1 / 3, 1 / 3, 1 / 3]

    m_dob = set_nocc["comparison_columns"][1]["m_probabilities"]
    assert pytest.approx(m_dob) == [1 / 3, 2 / 3]
