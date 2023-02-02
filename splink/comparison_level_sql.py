def great_circle_distance_km_sql(lat_l, lat_r, long_l, long_r):
    # Earth mean radius = 6371 km
    # see e.g. https://www.wolframalpha.com/input?i=earth+mean+radius+in+km
    EARTH_RADIUS_KM = 6371

    partial_distance_sql = f"""
        sin( radians({lat_l}) ) * sin( radians({lat_r}) ) +
        cos( radians({lat_l}) ) * cos( radians({lat_r}) )
            * cos( radians({long_r} - {long_l}) )
    """
    # The above should theoretically be in the range [-1, 1], but in practice
    # due to rounding errors some values can be slightly outside this range.
    # e.g. for (29.7517, -95.4054) then the above results in 1.0000000000000002
    # This causes an error in the acos function, so we need to clip the values
    # to the range [-1, 1]
    # See https://github.com/moj-analytical-services/splink/issues/1005
    partial_distance_sql = f"""
        case
            when ({partial_distance_sql}) > 1 then 1
            when ({partial_distance_sql}) < -1 then -1
            else ({partial_distance_sql})
        end
    """

    distance_km_sql = f"""
        cast(
            acos(
                {partial_distance_sql}
            ) * {EARTH_RADIUS_KM}
            as float
        )
    """
    return distance_km_sql
