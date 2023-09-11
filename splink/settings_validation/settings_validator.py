from __future__ import annotations

import logging
import re
from functools import reduce
from operator import and_

import sqlglot

from ..input_column import InputColumn, remove_quotes_from_identifiers
from ..misc import ensure_is_list

logger = logging.getLogger(__name__)


def remove_suffix(c):
    return re.sub("_[l|r]{1}$", "", c)


class SettingsValidator:
    """A base class for settings validation. This stores key variables and values
    from our central settings object and cleans adds functions to perform preliminary
    value cleaning.
    """

    def __init__(self, linker):
        self.linker = linker

    @property
    def _sql_dialect(self):
        if self.linker._settings_obj:
            return self.linker._settings_obj._sql_dialect
        else:
            return None

    @property
    def cols_to_retain(self):
        return self.clean_list_of_column_names(
            self.linker._settings_obj._additional_cols_to_retain
        )

    @property
    def uid(self):
        uid_as_tree = InputColumn(self.linker._settings_obj._unique_id_column_name)
        return self.clean_list_of_column_names(uid_as_tree)

    @property
    def blocking_rules(self):
        brs = self.linker._settings_obj._blocking_rules_to_generate_predictions
        return [br.blocking_rule for br in brs]

    @property
    def comparisons(self):
        return self.linker._settings_obj.comparisons

    @property
    def input_columns_by_df(self):
        """A dictionary containing all input dataframes and the columns located
        within.

        Returns:
            dict: A dictionary of the format `{"table_name": [col1, col2, ...]}
        """
        # For each input dataframe, grab the column names and create a dictionary
        # of the form: {table_name: [column_1, column_2, ...]}
        input_columns = {
            k: self.clean_list_of_column_names(v.columns)
            for k, v in self.linker._input_tables_dict.items()
        }

        return input_columns

    @property
    def input_columns(self):
        """
        Returns:
            set: The set intersection of all columns contained within each dataset.
        """
        return reduce(and_, self.input_columns_by_df.values())

    def clean_list_of_column_names(self, col_list, as_tree=True):
        """Clean a list of columns names by removing the quote characters
        that may exist.

        Args:
            col_list (list): A list of columns names.
            as_tree (bool): Whether the input columns are already SQL
                trees or need conversions.

        Returns:
            set: A set of column names without quotes.
        """
        if col_list is None:
            return ()  # needs to be a blank iterable

        col_list = ensure_is_list(col_list)
        if as_tree:
            col_list = [c.input_name_as_tree for c in col_list]
        return set(remove_quotes_from_identifiers(tree).sql() for tree in col_list)

    def remove_prefix_and_suffix_from_column(
        self, col_syntax_tree: sqlglot.expressions
    ):
        """Remove the prefix and suffix from a given sqlglot syntax tree
        and return it as a string of SQL.

        Args:
            col_syntax_tree (sqlglot.expressions): _description_

        Returns:
            str: A column without `l.` or `_l`
        """
        col_syntax_tree.args["table"] = None
        return remove_suffix(col_syntax_tree.sql())
