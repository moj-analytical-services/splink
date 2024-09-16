from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from copy import deepcopy
from functools import reduce
from operator import and_
from typing import TYPE_CHECKING, Iterable, List, Literal, overload

import sqlglot

from splink.internals.input_column import InputColumn
from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from splink.internals.settings import Settings


def remove_suffix(c: str) -> str:
    return re.sub("_[l|r]{1}$", "", c)


def find_columns_not_in_input_dfs(
    valid_input_dataframe_columns: Iterable[str], columns_to_check: Iterable[str] | str
) -> set[str]:
    """Identify missing columns in the input dataframe(s). This function
    does not apply any cleaning to the input column(s).
    """
    # the key to use when producing our warning logs
    if isinstance(columns_to_check, str):
        columns_to_check = {columns_to_check}

    return {col for col in columns_to_check if col not in valid_input_dataframe_columns}


def clean_and_find_columns_not_in_input_dfs(
    valid_input_dataframe_columns: Iterable[str],
    sqlglot_tree_columns_to_check: Sequence[sqlglot.Expression],
    sql_dialect: str,
) -> set[str]:
    """Clean a list of sqlglot column names to remove the prefix (l.)
    and suffix (_l) and then return any that are missing from the
    input dataframe(s).
    """
    sqlglot_tree_columns_to_check = deepcopy(sqlglot_tree_columns_to_check)
    cleaned_cols = set(
        remove_prefix_and_suffix_from_column(c, sql_dialect=sql_dialect)
        for c in sqlglot_tree_columns_to_check
    )
    return find_columns_not_in_input_dfs(valid_input_dataframe_columns, cleaned_cols)


def remove_prefix_and_suffix_from_column(
    col_syntax_tree: sqlglot.Expression,
    sql_dialect: str,
) -> str:
    """Remove the prefix and suffix from a given sqlglot syntax tree
    and return it as a string of SQL.

    Args:
        col_syntax_tree (sqlglot.Expression): _description_

    Returns:
        str: A column without `l.` and/or `_l`
    """
    col_syntax_tree.args["table"] = None
    return remove_suffix(col_syntax_tree.sql(sql_dialect))


def clean_list_of_column_names(col_list: List[InputColumn]) -> set[str]:
    """Clean a list of columns names by removing the quote characters
    that may exist.

    Args:
        col_list (list): A list of InputColumn classes.
    """
    if col_list is None:
        return ()  # needs to be a blank iterable

    return set((c.unquote().name for c in col_list))


@overload
def clean_user_input_columns(
    input_columns: dict[str, SplinkDataFrame], return_as_single_column: Literal[True]
) -> set[str]: ...


@overload
def clean_user_input_columns(
    input_columns: dict[str, SplinkDataFrame], return_as_single_column: Literal[False]
) -> dict[str, set[str]]: ...


def clean_user_input_columns(
    input_columns: dict[str, SplinkDataFrame], return_as_single_column: bool = True
) -> set[str] | dict[str, set[str]]:
    """A dictionary containing all input dataframes and the columns located
    within.

    Returns:
        dict: A dictionary of the format `{"table_name": [col1, col2, ...]}
    """
    # For each input dataframe, grab the column names and create a dictionary
    # of the form: {table_name: [column_1, column_2, ...]}
    cleaned_columns = {
        k: clean_list_of_column_names(v.columns) for k, v in input_columns.items()
    }

    if return_as_single_column:
        return reduce(and_, cleaned_columns.values())
    else:
        return cleaned_columns


class SettingsColumnCleaner:
    """
    A class that takes in a linker's settings object and spits out a series of
    cleaned up settings columns and SQL strings.
    """

    def __init__(
        self, settings_object: Settings, input_columns: dict[str, SplinkDataFrame]
    ):
        self.sqlglot_dialect = settings_object._sqlglot_dialect
        self._settings_obj = settings_object
        self.input_columns = clean_user_input_columns(
            input_columns, return_as_single_column=True
        )

    @property
    def cols_to_retain(self):
        return clean_list_of_column_names(
            self._settings_obj._additional_columns_to_retain
        )

    @property
    def uid(self):
        uid_as_tree = InputColumn(
            self._settings_obj.column_info_settings.unique_id_column_name,
            sqlglot_dialect_str=self.sqlglot_dialect,
        )
        return clean_list_of_column_names([uid_as_tree])

    @property
    def blocking_rules(self):
        brs = self._settings_obj._blocking_rules_to_generate_predictions
        return [br.blocking_rule_sql for br in brs]

    @property
    def comparisons(self):
        return self._settings_obj.comparisons
