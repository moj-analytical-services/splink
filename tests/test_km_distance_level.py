import pandas as pd

import splink.internals.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_simple_run(dialect):
    cl.DistanceInKMAtThresholds(
        lat_col="lat", long_col="long", km_thresholds=[1, 5, 10]
    ).create_comparison_dict(dialect)
    # print(
    #    cl.DistanceInKMAtThresholds(
    #        lat_col="latlong[0]", long_col="latlong[1]", km_thresholds=[1, 5, 10]
    #    ).as_dict()
    # )
    # print(
    #    cl.DistanceInKMAtThresholds(
    #        lat_col="ll['lat']", long_col="ll['long']", km_thresholds=[1, 5, 10]
    #    ).as_dict()
    # )


@mark_with_dialects_excluding()
def test_simple_run_cll(dialect):
    (
        cll.DistanceInKMLevel(
            lat_col="lat", long_col="long", km_threshold=1
        ).create_level_dict(dialect)
    )
    # cll.DistanceInKMLevel(
    #     lat_col="latlong[0]", long_col="latlong[1]", km_threshold=0.1
    # ).as_dict()
    # cll.DistanceInKMLevel(
    #     lat_col="ll['lat']", long_col="ll['long']", km_threshold=10
    # ).as_dict()


@mark_with_dialects_excluding()
def test_km_distance_levels(dialect, test_helpers):
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "name": "102 Petty France",
                "lat": 51.500516,
                "long": -0.133192,
            },
            {
                "unique_id": 2,
                "name": "10 South Colonnade",
                "lat": 51.504444,
                "long": -0.021389,
            },
            {
                "unique_id": 3,
                "name": "Houses of Parliament",
                "lat": 51.499479,
                "long": -0.124809,
            },
            {
                "unique_id": 4,
                "name": "5 Wellington Place",
                "lat": 53.796105,
                "long": -1.549725,
            },
            {
                "unique_id": 5,
                "name": "102 Petty France Duplicate",
                "lat": 51.500516,
                "long": -0.133192,
            },
            {
                "unique_id": 6,
                "name": "Splink",
                "lat": 53.3338,
                "long": -6.24488,
            },
        ]
    )

    settings_cl = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.DistanceInKMAtThresholds(
                lat_col="lat", long_col="long", km_thresholds=[0.1, 1, 10, 300]
            )
        ],
    }

    # For testing the cll version
    km_diff = {
        "output_column_name": "custom_lat_long",
        "comparison_levels": [
            {
                "sql_condition": "(lat_l IS NULL OR lat_r IS NULL) \n"
                "OR (long_l IS NULL OR long_r IS NULL)",
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            cll.DistanceInKMLevel(lat_col="lat", long_col="long", km_threshold=0.1),
            cll.DistanceInKMLevel(lat_col="lat", long_col="long", km_threshold=1),
            cll.DistanceInKMLevel(
                lat_col="lat",
                long_col="long",
                km_threshold=10,
            ),
            cll.DistanceInKMLevel(
                lat_col="lat",
                long_col="long",
                km_threshold=300,
            ),
            cll.ElseLevel(),
        ],
    }

    settings_cll = {"link_type": "dedupe_only", "comparisons": [km_diff]}

    df = helper.convert_frame(df)

    linker = helper.Linker(df, settings_cl, **helper.extra_linker_args())
    cl_df_e = linker.inference.predict().as_pandas_dataframe()
    linker = helper.Linker(df, settings_cll, **helper.extra_linker_args())
    cll_df_e = linker.inference.predict().as_pandas_dataframe()

    linker_outputs = {
        "cl": cl_df_e,
        "cll": cll_df_e,
    }

    # # Dict key: {size: gamma_level value}
    size_gamma_lookup = {0: 5, 1: 4, 2: 3, 3: 2, 4: 1}

    # Check gamma sizes are as expected
    for gamma, gamma_lookup in size_gamma_lookup.items():
        for linker_pred in linker_outputs.values():
            gamma_column_name_options = [
                # cl version
                "gamma_lat_long",
                # our custom cll version
                "gamma_custom_lat_long",
            ]
            gamma_column_name = linker_pred.columns[
                linker_pred.columns.str.contains("|".join(gamma_column_name_options))
            ][0]
            assert sum(linker_pred[gamma_column_name] == gamma) == gamma_lookup

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    gamma_lookup = {
        4: [(1, 5)],
        3: [(1, 3)],
        2: [(2, 5)],
        1: [(3, 4)],
    }

    for gamma, id_pairs in gamma_lookup.items():
        for left, right in id_pairs:
            gamma_column_name_options = [
                "gamma_custom_long_lat",
                "gamma_custom_lat_long",
            ]  # lat and long switch unpredictably
            gamma_column_name = linker_pred.columns[
                linker_pred.columns.str.contains("|".join(gamma_column_name_options))
            ][0]
            assert (
                linker_pred.loc[
                    (linker_pred.unique_id_l == left)
                    & (linker_pred.unique_id_r == right)
                ][gamma_column_name].values[0]
                == gamma
            )


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

    df = pd.DataFrame(data)

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [
            {
                "output_column_name": "lat_long",
                "comparison_levels": [
                    cll.NullLevel("lat"),  # no nulls in test data
                    cll.DistanceInKMLevel(
                        km_threshold=50,
                        lat_col="lat",
                        long_col="lon",
                    ),
                    cll.DistanceInKMLevel(
                        lat_col="lat_long['lat']",
                        long_col="lat_long['long']",
                        km_threshold=10000,
                    ),
                    cll.DistanceInKMLevel(
                        lat_col="lat_long_arr[1]",
                        long_col="lat_long_arr[2]",
                        km_threshold=100000,
                    ),
                    cll.ElseLevel(),
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, input_table_aliases="test", db_api=db_api)
    df_e = linker.inference.predict().as_pandas_dataframe()

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
