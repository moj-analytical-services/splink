import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def vertically_concatenate_sql(linker: "Linker") -> str:
    """
    Using `input_table_or_tables`, create a single table with the columns and
    rows required for linking.

    This table will later be the basis for the generation of pairwise record comparises,
    and will also be used to generate the term frequency adjustment tables.

    If multiple input tables are provided, this single table is the vertical
    concatenation of all the input tables.  In this case, a 'source_dataset' column
    is created.  This is used to uniquely identify rows in the vertical concatenation.
    Without it, ID collisions would be possible leading to ambiguity e.g. if several
    of the input tables have the same ID.
    """

    # Use column order from first table in dict
    df_obj = next(iter(linker._input_tables_dict.values()))
    columns = df_obj.columns_escaped

    select_columns_sql = ", ".join(columns)

    salting_reqiured = False

    # For data profiling, we need to vertically concat
    # but user may not have provided a settings dict yet
    if linker._settings_obj_ is None:
        source_dataset_col_req = True
    else:
        source_dataset_col_req = (
            linker._settings_obj._source_dataset_column_name_is_required
        )
        salting_reqiured = linker._settings_obj.salting_required

    if salting_reqiured:
        salt_sql = ", random() as __splink_salt"
    else:
        salt_sql = ""

    if source_dataset_col_req:
        sqls_to_union = []
        for df_obj in linker._input_tables_dict.values():
            sql = f"""
            select '{df_obj.templated_name}' as source_dataset, {select_columns_sql}
            {salt_sql}
            from {df_obj.physical_name}
            """
            sqls_to_union.append(sql)
        sql = " UNION ALL ".join(sqls_to_union)
    else:
        sql = f"""
            select {select_columns_sql}
            {salt_sql}
            from {df_obj.physical_name}
            """

    return sql
