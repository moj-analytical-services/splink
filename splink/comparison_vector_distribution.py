from typing import TYPE_CHECKING

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def comparison_vector_distribution_sql(linker: "Linker"):

    gamma_columns = [c._gamma_column_name for c in linker._settings_obj.comparisons]
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
