import pandas as pd
import pyarrow as pa

from splink.comparison_level_library import (
    columns_reversed_level,
    else_level,
    exact_match_level,
    null_level,
    distance_in_km_level,
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


def test_haversine_level():
    data = [
        {"id": 1, "lat": 22.730590, "lon": 9.388589},
        {"id": 2, "lat": 22.836322, "lon": 9.276112},
        {"id": 3, "lat": 37.770850, "lon": 95.689880},
        {"id": 4, "lat": -31.336319, "lon": 145.183685},
    ]
    # Add another the array version of the lat_long column
    for d in data:
        d["lat_long"] = {"lat": d["lat"], "long": d["lon"]}
        d["lat_long_arr"] = [d["lat"], d["lon"]]

    df = pa.Table.from_pylist(data)

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [
            {
                "output_column_name": "lat_long",
                "comparison_levels": [
                    null_level("lat"),  # no nulls in test data
                    distance_in_km_level(
                        km_threshold=50,
                        lat_col="lat",
                        long_col="lon",
                    ),
                    distance_in_km_level(
                        lat_col="lat_long['lat']",
                        long_col="lat_long['long']",
                        km_threshold=10000,
                    ),
                    distance_in_km_level(
                        lat_col="lat_long_arr[1]",
                        long_col="lat_long_arr[2]",
                        km_threshold=100000,
                    ),
                    else_level(),
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    linker = DuckDBLinker(df, settings, input_table_aliases="test")
    df_e = linker.predict().as_pandas_dataframe()

    row = dict(df_e.query("id_l == 1 and id_r == 2").iloc[0])
    assert row["gamma_lat_long"] == 3

    # id comparisons w/ dist < 10000km
    id_comb = {(1, 3), (2, 3), (3, 4)}
    for id_pair in id_comb:
        row = dict(df_e.query("id_l == {} and id_r == {}".format(*id_pair)).iloc[0])
        assert row["gamma_lat_long"] == 2

    id_comb = {(1, 4), (2, 4)}
    for id_pair in id_comb:
        row = dict(df_e.query("id_l == {} and id_r == {}".format(*id_pair)).iloc[0])
        assert row["gamma_lat_long"] == 1
