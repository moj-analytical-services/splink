def great_circle_distance_km_sql(lat_l, lat_r, long_l, long_r):
    # Earth mean radius = 6371 km
    # see e.g. https://www.wolframalpha.com/input?i=earth+mean+radius+in+km
    EARTH_RADIUS_KM = 6371

    partial_distance_sql = f"""
        sin( radians({lat_l}) ) * sin( radians({lat_r}) ) +
        cos( radians({lat_l}) ) * cos( radians({lat_r}) )
            * cos( radians({long_r} - {long_l}) )
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
