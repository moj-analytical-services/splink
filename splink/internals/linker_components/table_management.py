from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from splink.internals.database_api import AcceptableInputTableType
from splink.internals.input_column import InputColumn
from splink.internals.misc import (
    ascii_uid,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.term_frequencies import (
    colname_to_tf_tablename,
    term_frequencies_for_single_column_sql,
)
from splink.internals.vertically_concatenate import (
    enqueue_df_concat,
)

if TYPE_CHECKING:
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


class LinkerTableManagement:
    """Register Splink tables against your database backend and manage the Splink cache.
    Accessed via `linker.table_management`.
    """

    def __init__(self, linker: Linker):
        self._linker = linker

    def compute_tf_table(self, column_name: str) -> SplinkDataFrame:
        """Compute a term frequency table for a given column and persist to the database

        This method is useful if you want to pre-compute term frequency tables e.g.
        so that real time linkage executes faster, or so that you can estimate
        various models without having to recompute term frequency tables each time

        Examples:

            Real time linkage
            ```py
            linker = Linker(df, settings="saved_settings.json", db_api=db_api)
            linker.table_management.compute_tf_table("surname")
            linker.inference.compare_two_records(record_left, record_right)
            ```
            Pre-computed term frequency tables
            ```py
            linker = Linker(df, db_api)
            df_first_name_tf = linker.table_management.compute_tf_table("first_name")
            df_first_name_tf.write.parquet("folder/first_name_tf")
            >>>
            # On subsequent data linking job, read this table rather than recompute
            df_first_name_tf = pd.read_parquet("folder/first_name_tf")
            linker.table_management.register_term_frequency_lookup(
                df_first_name_tf, "first_name"
            )

            ```


        Args:
            column_name (str): The column name in the input table

        Returns:
            SplinkDataFrame: The resultant table as a splink data frame
        """

        input_col = InputColumn(
            column_name,
            column_info_settings=self._linker._settings_obj.column_info_settings,
            sqlglot_dialect_str=self._linker._settings_obj._sql_dialect_str,
        )
        tf_tablename = colname_to_tf_tablename(input_col)
        cache = self._linker._intermediate_table_cache

        if tf_tablename in cache:
            tf_df = cache.get_with_logging(tf_tablename)
        else:
            pipeline = CTEPipeline()
            pipeline = enqueue_df_concat(self._linker, pipeline)
            sql = term_frequencies_for_single_column_sql(input_col)
            pipeline.enqueue_sql(sql, tf_tablename)
            tf_df = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
            self._linker._intermediate_table_cache[tf_tablename] = tf_df

        return tf_df

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
        self.delete_tables_created_by_splink_from_db()

        # As a result, any previously cached tables will not be found
        self._linker._intermediate_table_cache.invalidate_cache()

    def register_table_input_nodes_concat_with_tf(
        self, input_data: AcceptableInputTableType, overwrite: bool = False
    ) -> SplinkDataFrame:
        """Register a pre-computed version of the input_nodes_concat_with_tf table that
        you want to re-use e.g. that you created in a previous run.

        This method allows you to register this table in the Splink cache so it will be
        used rather than Splink computing this table anew.

        Args:
            input_data (AcceptableInputTableType): The data you wish to register. This
                can be either a dictionary, pandas dataframe, pyarrow table or a spark
                dataframe.
            overwrite (bool): Overwrite the table in the underlying database if it
                exists.

        Returns:
            SplinkDataFrame: An abstraction representing the table created by the sql
                pipeline
        """

        table_name_physical = "__splink__df_concat_with_tf_" + self._linker._cache_uid
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        splink_dataframe.templated_name = "__splink__df_concat_with_tf"

        self._linker._intermediate_table_cache["__splink__df_concat_with_tf"] = (
            splink_dataframe
        )
        return splink_dataframe

    def register_table_predict(self, input_data, overwrite=False):
        """Register a pre-computed version of the prediction table for use in Splink.

        This method allows you to register a pre-computed prediction table in the Splink
        cache so it will be used rather than Splink computing the table anew.

        Examples:
            ```py
            predict_df = pd.read_parquet("path/to/predict_df.parquet")
            predict_as_splinkdataframe = linker.table_management.register_table_predict(predict_df)
            clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
                predict_as_splinkdataframe, threshold_match_probability=0.75
            )
            ```

        Args:
            input_data (AcceptableInputTableType): The data you wish to register. This
                can be either a dictionary, pandas dataframe, pyarrow table, or a spark
                dataframe.
            overwrite (bool, optional): Overwrite the table in the underlying database
                if it exists. Defaults to False.

        Returns:
            (SplinkDataFrame): An abstraction representing the table created by the SQL
                pipeline.
        """  # noqa: E501
        table_name_physical = "__splink__df_predict_" + self._linker._cache_uid
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        self._linker._intermediate_table_cache["__splink__df_predict"] = (
            splink_dataframe
        )
        splink_dataframe.templated_name = "__splink__df_predict"
        return splink_dataframe

    def register_term_frequency_lookup(self, input_data, col_name, overwrite=False):
        """Register a pre-computed term frequency lookup table for a given column.

        This method allows you to register a term frequency table in the Splink
        cache for a specific column. This table will then be used during linkage
        rather than computing the term frequency table anew from your input data.

        Args:
            input_data (AcceptableInputTableType): The data representing the term
                frequency table. This can be either a dictionary, pandas dataframe,
                pyarrow table, or a spark dataframe.
            col_name (str): The name of the column for which the term frequency
                lookup table is being registered.
            overwrite (bool, optional): Overwrite the table in the underlying
                database if it exists. Defaults to False.

        Returns:
            (SplinkDataFrame): An abstraction representing the registered term
                frequency table.

        Examples:
            ```py
            tf_table = [
                {"first_name": "theodore", "tf_first_name": 0.012},
                {"first_name": "alfie", "tf_first_name": 0.013},
            ]
            tf_df = pd.DataFrame(tf_table)
            linker.table_management.register_term_frequency_lookup(
                tf_df,
                "first_name"
            )
            ```
        """

        input_col = InputColumn(
            col_name,
            column_info_settings=self._linker._settings_obj.column_info_settings,
            sqlglot_dialect_str=self._linker._settings_obj._sql_dialect_str,
        )

        table_name_templated = colname_to_tf_tablename(input_col)
        table_name_physical = f"{table_name_templated}_{self._linker._cache_uid}"
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        self._linker._intermediate_table_cache[table_name_templated] = splink_dataframe
        splink_dataframe.templated_name = table_name_templated
        return splink_dataframe

    def register_labels_table(self, input_data, overwrite=False):
        table_name_physical = "__splink__df_labels_" + ascii_uid(8)
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        splink_dataframe.templated_name = "__splink__df_labels"
        return splink_dataframe

    def delete_tables_created_by_splink_from_db(self):
        self._linker._db_api.delete_tables_created_by_splink_from_db()

    def register_table(
        self,
        input_table: AcceptableInputTableType,
        table_name: str,
        overwrite: bool = False,
    ) -> SplinkDataFrame:
        """
        Register a table to your backend database, to be used in one of the
        splink methods, or simply to allow querying.

        Tables can be of type: dictionary, record level dictionary,
        pandas dataframe, pyarrow table and in the spark case, a spark df.

        Examples:
            ```py
            test_dict = {"a": [666,777,888],"b": [4,5,6]}
            linker.table_management.register_table(test_dict, "test_dict")
            linker.misc.query_sql("select * from test_dict")
            ```

        Args:
            input_table: The data you wish to register. This can be either a dictionary,
                pandas dataframe, pyarrow table or a spark dataframe.
            table_name (str): The name you wish to assign to the table.
            overwrite (bool): Overwrite the table in the underlying database if it
                exists

        Returns:
            SplinkDataFrame: An abstraction representing the table created by the sql
                pipeline
        """

        return self._linker._db_api.register_table(input_table, table_name, overwrite)
