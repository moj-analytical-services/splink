import splink.internals.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
from tests.decorator import mark_with_dialects_excluding
from tests.utils import (
    assert_id_pair_has_gamma_value,
    assert_number_of_rows_with_gamma_value,
)


@mark_with_dialects_excluding()
def test_simple_run(dialect):
    cl.DistanceInKMAtThresholds(
        lat_col="lat", long_col="long", km_thresholds=[1, 5, 10]
    ).create_comparison_dict(dialect)


@mark_with_dialects_excluding()
def test_simple_run_cll(dialect):
    (
        cll.DistanceInKMLevel(
            lat_col="lat", long_col="long", km_threshold=1
        ).create_level_dict(dialect)
    )


@mark_with_dialects_excluding()
def test_km_distance_levels(dialect, test_helpers):
    helper = test_helpers[dialect]

    data = [
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

    linker = helper.linker_with_registration([data], settings_cl)
    cl_df_e = linker.inference.predict()
    linker = helper.linker_with_registration([data], settings_cll)
    cll_df_e = linker.inference.predict()

    linker_outputs = {
        "cl": {
            "df_pred": cl_df_e,
            "gamma_colname": "gamma_lat_long",
        },
        "cll": {
            "df_pred": cll_df_e,
            "gamma_colname": "gamma_custom_lat_long",
        },
    }

    # # Dict key: {size: gamma_level value}
    size_gamma_lookup = {0: 5, 1: 4, 2: 3, 3: 2, 4: 1}

    # Check gamma sizes are as expected
    for gamma_level, gamma_lookup in size_gamma_lookup.items():
        for linker_pred in linker_outputs.values():
            assert_number_of_rows_with_gamma_value(
                linker_pred["df_pred"],
                linker_pred["gamma_colname"],
                gamma_level,
                gamma_lookup,
            )

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    gamma_lookup = {
        4: [(1, 5)],
        3: [(1, 3)],
        2: [(2, 5)],
        1: [(3, 4)],
    }

    for gamma_level, id_pairs in gamma_lookup.items():
        for id_pair in id_pairs:
            for linker_pred in linker_outputs.values():
                assert_id_pair_has_gamma_value(
                    linker_pred["df_pred"],
                    linker_pred["gamma_colname"],
                    gamma_level,
                    id_pair,
                )


def test_haversine_level():
    data = [
        {"unique_id": 1, "lat": 22.730590, "lon": 9.388589},
        {"unique_id": 2, "lat": 22.836322, "lon": 9.276112},
        {"unique_id": 3, "lat": 37.770850, "lon": 95.689880},
        {"unique_id": 4, "lat": -31.336319, "lon": 145.183685},
    ]
    # Add another the array version of the lat_long column
    for d in data:
        d["lat_long"] = {"lat": d["lat"], "long": d["lon"]}
        d["lat_long_arr"] = [d["lat"], d["lon"]]

    settings = {
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

    df_sdf = db_api.register(data, dataset_display_name="test")
    linker = Linker(df_sdf, settings)
    df_e = linker.inference.predict()

    assert_id_pair_has_gamma_value(df_e, "gamma_lat_long", 3, (1, 2))

    # id comparisons w/ dist < 10000km
    id_comb = {(1, 3), (2, 3), (3, 4)}
    for id_pair in id_comb:
        assert_id_pair_has_gamma_value(df_e, "gamma_lat_long", 2, id_pair)

    id_comb = {(1, 4), (2, 4)}
    for id_pair in id_comb:
        assert_id_pair_has_gamma_value(df_e, "gamma_lat_long", 1, id_pair)
