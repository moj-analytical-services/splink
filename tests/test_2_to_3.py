import pandas as pd
import os

from splink.convert_v2_to_v3 import convert_settings_from_v2_to_v3
from splink.duckdb.duckdb_linker import DuckDBLinker


def test_2_to_3(tmp_path):

    sql_col1 = """
    case
        when dob_l is null or dob_r is null then -1
        when dob_l = dob_r then 1
        else 0 end as gamma_dob'
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
            },
            {
                "custom_name": "name_inversion_forname",
                "case_expression": sql_custom_name,
                "custom_columns_used": ["forename", "surname"],
                "num_levels": 4,
            },
        ],
        "blocking_rules": ["l.first_name = r.first_name"],
    }

    converted = convert_settings_from_v2_to_v3(settings)
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    DuckDBLinker(
        df,
        converted,
        connection=os.path.join(tmp_path, "duckdb.db"),
        output_schema="splink_in_duckdb",
    )
