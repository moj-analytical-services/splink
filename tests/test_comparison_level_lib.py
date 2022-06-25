import pandas as pd

from splink.duckdb.duckdb_comparison_level_library import (
    columns_reversed_level,
    else_level,
    exact_match_level,
    null_level,
)

from splink.duckdb.duckdb_linker import DuckDBLinker


def test_column_reversal():
    data = [
        {"id": 1, "forename": "John", "surname": "Smith", "full_name": "John Smith"},
        {"id": 2, "forename": "Smith", "surname": "John", "full_name": "Smith John"},
        {"id": 3, "forename": "Rob", "surname": "Jones", "full_name": "Rob Jones"},
        {"id": 4, "forename": "Rob", "surname": "Jones", "full_name": "Rob Jones"},
    ]

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [
            {
                "output_column_name": "full_name",
                "comparison_levels": [
                    null_level("full_name"),
                    exact_match_level("full_name"),
                    columns_reversed_level("forename", "surname"),
                    else_level(),
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    df = pd.DataFrame(data)

    linker = DuckDBLinker(df, settings)
    df_e = linker.predict().as_pandas_dataframe()

    row = dict(df_e.query("id_l == 1 and id_r == 2").iloc[0])
    assert row["gamma_full_name"] == 1

    row = dict(df_e.query("id_l == 3 and id_r == 4").iloc[0])
    assert row["gamma_full_name"] == 2
