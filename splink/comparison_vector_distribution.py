def comparison_vector_distribution_sql(linker):

    gamma_columns = [c.gamma_column_name for c in linker.settings_obj.comparisons]
    groupby_cols = " , ".join(gamma_columns)
    gam_concat = " || ',' || ".join(gamma_columns)

    case_tem = "(case when {g} = -1 then 0 when {g} = 0 then -1 else {g} end)"
    sum_gam = " + ".join([case_tem.format(g=c) for c in gamma_columns])

    sql = f"""
    select {gam_concat} as gam_concat,
    {sum_gam} as sum_gam,
    count(*) as count_rows_in_comparison_vector_group,
    cast(count(*) as float)
        /(select count(*) from __splink__df_predict) as proportion_of_comparisons,
    {groupby_cols}
    from __splink__df_predict
    group by {groupby_cols}
    order by {sum_gam}
    """

    return sql


#   proportion_of_comparisons: 0.0005186722
#   cumulative_comparisons: 0.0005186722

#   gamma_surname_std: 0
#   gamma_forename1_std: 0
#   gamma_forename2_std: -1
#   gamma_occupation: 0
#   gamma_dob: 0
#   gamma_custom_postcode_distance_comparison: 0
#   gam_concat: "0,0,-1,0,0,0"
#   sum_gam: -5
#   count: 2
#   proportion_of_comparisons: 0.0005186722
#   cumulative_comparisons: 0.0005186722
#   match_weight: -42.8357664485
#   match_probability: 0
#   source_dataset_l: "synthentic_data"
#   unique_id_l: "Q18783557-13"
#   source_dataset_r: "synthentic_data"
#   unique_id_r: "Q7341531-2"
#   surname_std_l: "robertson"
#   surname_std_r: "andrews"
#   forename1_std_l: "andrea"
#   forename1_std_r: "robert"
#   forename2_std_l: null
#   forename2_std_r: null
#   forename3_std_l: null
#   forename3_std_r: null
#   forename4_std_l: null
#   forename4_std_r: null
#   forename5_std_l: null
#   forename5_std_r: null
#   occupation_l: "painter"
#   occupation_r: "poet"
#   dob_l: "1747-10-14"
#   dob_r: "1723-01-01"
#   postcode_l: "AB10 1TT"
#   postcode_r: "SA62 6JS"
#   lat_lng_l: Array(2) [57.143026, -2.112349]
#   lat_lng_r: Array(2) [51.845886, -5.010543]
#   birth_place_l: null
#   birth_place_r: "Camrose"
#   cluster_l: "Q18783557"
#   cluster_r: "Q7341531"
#   match_key: "8"
#   row_example_index: 2
