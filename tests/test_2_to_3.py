import os

import pandas as pd

from splink.convert_v2_to_v3 import convert_settings_from_v2_to_v3
from splink.database_api import DuckDBAPI
from splink.linker import Linker


def test_2_to_3(tmp_path):
    sql_col1 = """
    case
        when col_1_l is null or col_1_r is null then -1
        when col_1_l = col_1_r then 1
        else 0 end as gamma_col_1
    """

    sql_col2 = """
    case
        when col_2_l = col_2_r then 1
        else 0 end
    """

    sql_custom_name = """
    case
        when (forename_l is null or forename_r is null) and
             (surname_l is null or surname_r is null)  then -1
        when forename_l = forename_r and surname_l = surname_r then 3
        when forename_l = forename_r then 2
        when surname_l = surname_r then 1
        else 0
    end
    """
    settings = {
        "link_type": "dedupe_only",
        "comparison_columns": [
            {
                "col_name": "col_1",
                "m_probabilities": [0.3, 0.7],
                "u_probabilities": [0.9, 0.1],
                "case_expression": sql_col1,
            },
            {
                "col_name": "col_2",
                "term_frequency_adjustments": True,
                "case_expression": sql_col2,
            },
            {
                "custom_name": "name_inversion_forname",
                "case_expression": sql_custom_name,
                "custom_columns_used": ["forename", "surname"],
                "num_levels": 4,
                "m_probabilities": [0.1, 0.1, 0.1, 0.7],
                "u_probabilities": [0.9, 0.02, 0.02, 0.06],
            },
        ],
        "blocking_rules": ["l.first_name = r.first_name"],
    }

    converted = convert_settings_from_v2_to_v3(settings)
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    db_api = DuckDBAPI(
        connection=os.path.join(tmp_path, "duckdb.db"),
        output_schema="splink_in_duckdb",
    )

    Linker(
        df,
        converted,
        database_api=db_api,
    )
