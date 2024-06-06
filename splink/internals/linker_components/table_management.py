from __future__ import annotations

import logging

from splink.internals.input_column import InputColumn
from splink.internals.misc import (
    ascii_uid,
)
from splink.internals.term_frequencies import (
    colname_to_tf_tablename,
)

logger = logging.getLogger(__name__)


class LinkerTableManagement:
    def __init__(self, linker):
        self._linker = linker

    def invalidate_cache(self):
        """Invalidate the Splink cache.  Any previously-computed tables
        will be recomputed.
        This is useful, for example, if the input data tables have changed.
        """

        # Nothing to delete
        if len(self._linker._intermediate_table_cache) == 0:
            return

        # Before Splink executes a SQL command, it checks the cache to see
        # whether a table already exists with the name of the output table

        # This function has the effect of changing the names of the output tables
        # to include a different unique id

        # As a result, any previously cached tables will not be found
        self._linker._cache_uid = ascii_uid(8)

        # Drop any existing splink tables from the database
        # Note, this is not actually necessary, it's just good housekeeping
        self._linker.delete_tables_created_by_splink_from_db()

        # As a result, any previously cached tables will not be found
        self._linker._intermediate_table_cache.invalidate_cache()

    def register_table_input_nodes_concat_with_tf(self, input_data, overwrite=False):
        """Register a pre-computed version of the input_nodes_concat_with_tf table that
        you want to re-use e.g. that you created in a previous run

        This method allowed you to register this table in the Splink cache
        so it will be used rather than Splink computing this table anew.

        Args:
            input_data: The data you wish to register. This can be either a dictionary,
                pandas dataframe, pyarrow table or a spark dataframe.
            overwrite (bool): Overwrite the table in the underlying database if it
                exists
        """

        table_name_physical = "__splink__df_concat_with_tf_" + self._linker._cache_uid
        splink_dataframe = self._linker.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        splink_dataframe.templated_name = "__splink__df_concat_with_tf"

        self._linker._intermediate_table_cache["__splink__df_concat_with_tf"] = (
            splink_dataframe
        )
        return splink_dataframe

    def register_table_predict(self, input_data, overwrite=False):
        table_name_physical = "__splink__df_predict_" + self._linker._cache_uid
        splink_dataframe = self._linker.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        self._linker._intermediate_table_cache["__splink__df_predict"] = (
            splink_dataframe
        )
        splink_dataframe.templated_name = "__splink__df_predict"
        return splink_dataframe

    def register_term_frequency_lookup(self, input_data, col_name, overwrite=False):
        input_col = InputColumn(
            col_name,
            column_info_settings=self._linker._settings_obj.column_info_settings,
            sql_dialect=self._linker._settings_obj._sql_dialect,
        )
        table_name_templated = colname_to_tf_tablename(input_col)
        table_name_physical = f"{table_name_templated}_{self._linker._cache_uid}"
        splink_dataframe = self._linker.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        self._linker._intermediate_table_cache[table_name_templated] = splink_dataframe
        splink_dataframe.templated_name = table_name_templated
        return splink_dataframe

    def register_labels_table(self, input_data, overwrite=False):
        table_name_physical = "__splink__df_labels_" + ascii_uid(8)
        splink_dataframe = self._linker.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        splink_dataframe.templated_name = "__splink__df_labels"
        return splink_dataframe
