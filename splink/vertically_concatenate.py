from __future__ import annotations

import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .database_api import DatabaseAPI


def vertically_concatenate_sql(
    input_tables: list, db_api: DatabaseAPI, include_source_dataset_column=True
) -> str:
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
    df_obj = input_tables[0]
    columns = df_obj.columns_escaped

    select_columns_sql = ", ".join(columns)

    # should think about how these will work:
    salting_required = True
    # fix this for now:
    # source_dataset_col_req = True

    # TODO: does it make sense to set this on db api? e.g.:
    # if db_api.salting_required:
    #     salting_required = True
    # see https://github.com/duckdb/duckdb/discussions/9710
    # in duckdb to parallelise we need salting

    if salting_required:
        salt_sql = ", random() as __splink_salt"
    else:
        salt_sql = ""

    sqls_to_union = []

    create_sds_if_needed = ""

    for df_obj in input_tables:
        if include_source_dataset_column:
            create_sds_if_needed = f"'{df_obj.templated_name}' as source_dataset,"
        sql = f"""
        select
        {create_sds_if_needed}
        {select_columns_sql}
        {salt_sql}
        from {df_obj.physical_name}
        """
        sqls_to_union.append(sql)
    sql = " UNION ALL ".join(sqls_to_union)

    return sql
