from splink.internals.parse_sql import get_columns_used_from_sql


def test_get_columns_used():
    sql = """
    jaro_winkler_sim(mytable.surname_l, surname_r) > 0.99 or
    substr(mytable.surname_l || initial_l ,1,2) = substr(surname_r || initial_r,1,2)
    """

    assert set(get_columns_used_from_sql(sql)) == set(
        ["surname_l", "surname_r", "initial_l", "initial_r"]
    )

    assert set(get_columns_used_from_sql(sql, retain_table_prefix=True)) == set(
        ["mytable.surname_l", "surname_r", "initial_l", "initial_r"]
    )

    sql = """
    lat_lng_uncommon_l['lat'] - lat_lng_uncommon_r['lat']
    """
    assert set(get_columns_used_from_sql(sql)) == set(
        [
            "lat_lng_uncommon_l",
            "lat_lng_uncommon_r",
        ]
    )

    sql = """
    transform(latlongexplode(lat_lng_arr_uncommon_l,lat_lng_arr_uncommon_r ),
    x -> sin(radians(x['place2']['lat'] - x['place1']['lat'])) )
    """

    assert set(get_columns_used_from_sql(sql)) == set(
        [
            "lat_lng_arr_uncommon_l",
            "lat_lng_arr_uncommon_r",
        ]
    )

    sql = "AGGREGATE(cities, 0, (x, y) -> x + length(y))"

    assert set(get_columns_used_from_sql(sql)) == set(
        [
            "cities",
        ]
    )

    sql = "AGGREGATE(cities, 0, x ->  length(x['a']))"

    assert set(get_columns_used_from_sql(sql)) == set(
        [
            "cities",
        ]
    )

    sql = """
    ARRAY_MIN(TRANSFORM(LATLONGEXPLODE(lat_lng_arr_uncommon_l, lat_lng_arr_uncommon_r),
    (x) -> (CAST(ATAN2(SQRT((POW(SIN(RADIANS(x['place2']['lat'] - x['place1']['lat']))
    / 2, 2) + COS(RADIANS(x['place1']['lat'])) * COS(RADIANS(x['place2']['lat']))
    * POW(SIN(RADIANS(x['place2']['long'] - x['place1']['long']) / 2), 2))),
    SQRT(-1 * (POW(SIN(RADIANS(x['place2']['lat'] - x['place1']['lat'])) / 2, 2) +
    COS(RADIANS(x['place1']['lat'])) * COS(RADIANS(x['place2']['lat'])) *
    POW(SIN(RADIANS(x['place2']['long'] - x['place1']['long']) / 2), 2)) + 1))
    * 12742 AS FLOAT)))) < 5
    """

    assert set(get_columns_used_from_sql(sql)) == set(
        ["lat_lng_arr_uncommon_l", "lat_lng_arr_uncommon_r"]
    )
