# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/splink/pull/107

import logging
from typing import List, TYPE_CHECKING

from .input_column import InputColumn, remove_quotes_from_identifiers

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker

logger = logging.getLogger(__name__)


def input_col_to_tf_tablename(input_column: InputColumn, table_prefix):
    input_col_no_quotes = remove_quotes_from_identifiers(
        input_column.input_name_as_tree
    )

    input_column = input_col_no_quotes.sql().replace(" ", "_")
    return f"{table_prefix}tf_{input_column}"


def term_frequencies_for_single_column_sql(input_column: InputColumn, table_name):

    col_name = input_column.name()

    sql = f"""
    select
    {col_name}, cast(count(*) as double) / (select
        count({col_name}) as total from {table_name})
            as {input_column.tf_name()}
    from {table_name}
    where {col_name} is not null
    group by {col_name}
    """

    return sql


def _join_tf_to_input_df_sql(linker: "Linker"):

    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns
    table_prefix = linker._table_prefix

    select_cols = []

    for col in tf_cols:
        tbl = input_col_to_tf_tablename(col, table_prefix)
        tf_col = col.tf_name()
        select_cols.append(f"{tbl}.{tf_col}")

    select_cols.insert(0, f"{table_prefix}nodes_concat.*")
    select_cols = ", ".join(select_cols)

    templ = "left join {tbl} on {table_prefix}nodes_concat.{col} = {tbl}.{col}"

    left_joins = [
        templ.format(
            tbl=input_col_to_tf_tablename(col, table_prefix),
            table_prefix=table_prefix,
            col=col.name(),
        )
        for col in tf_cols
    ]
    left_joins = " ".join(left_joins)

    sql = f"""
    select {select_cols}
    from {table_prefix}nodes_concat
    {left_joins}
    """

    return sql


def compute_all_term_frequencies_sqls(linker: "Linker") -> List[dict]:

    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns
    table_prefix = linker._table_prefix

    if not tf_cols:
        return [
            {
                "sql": f"select * from {table_prefix}nodes_concat",
                "output_table_name": f"{table_prefix}nodes_concat_with_tf",
            }
        ]

    sqls = []
    for tf_col in tf_cols:
        tf_table_name = input_col_to_tf_tablename(tf_col, table_prefix)

        if not linker._table_exists_in_database(tf_table_name):
            tablename = f"{table_prefix}nodes_concat"
            sql = term_frequencies_for_single_column_sql(tf_col, tablename)
            sql = {
                "sql": sql,
                "output_table_name": input_col_to_tf_tablename(tf_col, table_prefix),
            }
            sqls.append(sql)

    sql = _join_tf_to_input_df_sql(linker)
    sql = {
        "sql": sql,
        "output_table_name": f"{table_prefix}nodes_concat_with_tf",
    }
    sqls.append(sql)

    return sqls
