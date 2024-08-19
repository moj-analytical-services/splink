import pytest
from duckdb import connect

from splink.internals.comparison_level_sql import great_circle_distance_km_sql

# approx 1/4 of max circumference on Earth in spherical model,
# using 12742 km diameter (as in splink.misc)
QUARTER_CIRCUMFERENCE_KM = 10007


@pytest.mark.parametrize(
    "lat_l,long_l,lat_r,long_r,expected_distance",
    [
        # quarter-turn apart
        (0, 0, 0, 90, QUARTER_CIRCUMFERENCE_KM),
        (0, 0, 90, 0, QUARTER_CIRCUMFERENCE_KM),
        (-25, 0, 0, 90, QUARTER_CIRCUMFERENCE_KM),
        (45, -30, 45, 150, QUARTER_CIRCUMFERENCE_KM),
        # same points
        (40, -20, 40, -20, 0),
        # same points, but due to rounding of floating point numbers,
        # the argument to the ACOS function can be larger than 1,
        # which causes an Out of Range error
        # See https://github.com/moj-analytical-services/splink/issues/1005
        (29.7517, -95.4054, 29.7517, -95.4054, 0),
        # antipodal points
        (20, 40, -20, -140, 2 * QUARTER_CIRCUMFERENCE_KM),
        (89, -60, -89, 120, 2 * QUARTER_CIRCUMFERENCE_KM),
        # some arbitrary point-spreads
        (51.484, -0.115, -37.82, 144.983, 16905),
        (-78.525483, -85.617147, 68.919500, -29.898533, 16783),
        (37.814056, -122.477898, 37.825531, -122.479236, 1.2814),
        # 0.2 degree difference
        (89.9, 0, 89.9, 180, 22.24),
        (90, 30, 89.8, 40, 22.24),
        (0, -24, 0, -24.2, 22.24),
    ],
)
def test_lat_long_distance_formula(lat_l, long_l, lat_r, long_r, expected_distance):
    con = connect()

    sql = great_circle_distance_km_sql(lat_l, lat_r, long_l, long_r)
    res = con.execute(f"SELECT {sql} AS dist_km").fetchone()
    # allow relative error of 0.01% (as expected distances are not precise),
    # or absolute error of 0.1% i.e. within 1 metre
    assert res[0] == pytest.approx(expected_distance, rel=1e-4, abs=1e-3)
