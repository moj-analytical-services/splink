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
    compute_df_concat_with_tf as _compute_df_concat_with_tf,
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

    def compute_df_concat_with_tf(self, cache: bool = True) -> SplinkDataFrame:
        """Compute concatenated input records with term frequency columns.

        This method computes the vertically concatenated input table with
        term frequency columns added for all columns specified in the settings.

        If cache=True (default), this stores the result in the intermediate
        table cache under the key "__splink__df_concat_with_tf", so other
        methods (e.g. predict, training) can reuse it.

        Args:
            cache (bool, optional): If True (default), cache the result in
                `_intermediate_table_cache` under the key
                "__splink__df_concat_with_tf". Set to False to compute without
                populating the cache.

        Returns:
            SplinkDataFrame: The concatenated input records with term frequency
                columns.

        Examples:
            ```py
            # Compute and cache for later use
            df_concat_with_tf = linker.table_management.compute_df_concat_with_tf()

            # Save to disk for reuse
            df_concat_with_tf.as_pandas_dataframe().to_parquet("checkpoint.parquet")

            # Later, load and register the precomputed table
            df = pd.read_parquet("checkpoint.parquet")
            linker.table_management.register_table_input_nodes_concat_with_tf(df)
            ```
        """
        pipeline = CTEPipeline()

        # _compute_df_concat_with_tf already checks cache and populates it
        df = _compute_df_concat_with_tf(self._linker, pipeline)

        # Ensure templated name is set correctly
        df.templated_name = "__splink__df_concat_with_tf"

        # If cache=False, remove from cache (it was added by _compute_df_concat_with_tf)
        if not cache:
            self._linker._intermediate_table_cache.pop(
                "__splink__df_concat_with_tf", None
            )
        else:
            # Ensure it's in the cache (may already be, but this is explicit)
            self._linker._intermediate_table_cache["__splink__df_concat_with_tf"] = df

        return df

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
            tf_df.templated_name = tf_tablename
            self._linker._intermediate_table_cache[tf_tablename] = tf_df

        return tf_df

    def compute_blocked_pairs(self) -> SplinkDataFrame:
        """Compute and cache the table of blocked record pairs.

        This method generates record pairs using the blocking rules specified
        in `blocking_rules_to_generate_predictions` from your settings. The
        resulting table contains pairs of record IDs that will be compared
        during prediction.

        The result is stored in the intermediate table cache under the key
        `__splink__blocked_id_pairs`, so subsequent calls to `predict()` or
        `deterministic_link()` can reuse it instead of recomputing.

        This is useful when you want to:
        - Pre-compute blocked pairs once and run multiple predictions with
          different thresholds
        - Save the blocked pairs to disk for later reuse
        - Inspect the blocked pairs before running predictions

        Note:
            If you change the blocking rules in your settings, you must call
            `invalidate_blocked_pairs()` before calling this method again to
            ensure the cached table reflects the new rules.

        Returns:
            SplinkDataFrame: The table of blocked record pairs, with columns:
                - `match_key`: Integer identifying which blocking rule generated
                  this pair
                - `join_key_l`: Composite unique ID of the left record
                - `join_key_r`: Composite unique ID of the right record

        Examples:
            ```py
            # Compute blocked pairs once
            blocked_pairs = linker.table_management.compute_blocked_pairs()

            # Run multiple predictions with different thresholds
            df_09 = linker.inference.predict(threshold_match_probability=0.9)
            df_08 = linker.inference.predict(threshold_match_probability=0.8)

            # Save to disk for later reuse
            blocked_pairs.as_pandas_dataframe().to_parquet("blocked_pairs.parquet")
            ```
        """
        # Delegate to inference for the actual computation
        blocked_pairs = self._linker.inference.compute_blocked_pairs()

        # Store in cache
        self._linker._intermediate_table_cache["__splink__blocked_id_pairs"] = (
            blocked_pairs
        )

        return blocked_pairs

    def register_blocked_pairs(
        self, input_data: AcceptableInputTableType, overwrite: bool = False
    ) -> SplinkDataFrame:
        """Register a pre-computed blocked pairs table.

        This method allows you to register a previously computed blocked pairs
        table (e.g., loaded from Parquet) into the Splink cache. This table
        will then be used by `predict()` and `deterministic_link()` instead
        of recomputing blocked pairs.

        The input table must have the same schema as the output of
        `compute_blocked_pairs()`:
        - `match_key`: Integer identifying which blocking rule generated this pair
        - `join_key_l`: Composite unique ID of the left record
        - `join_key_r`: Composite unique ID of the right record

        Note:
            The registered blocked pairs must have been computed using the same
            blocking rules as currently specified in your settings. If you change
            the blocking rules, you must recompute and re-register the blocked
            pairs.

        Args:
            input_data (AcceptableInputTableType): The blocked pairs data. Can be
                a dictionary, pandas DataFrame, PyArrow table, or Spark DataFrame.
            overwrite (bool, optional): If True, overwrite any existing table with
                the same name in the database. Defaults to False.

        Returns:
            SplinkDataFrame: The registered blocked pairs table.

        Examples:
            ```py
            # Load previously saved blocked pairs
            blocked_pairs_pd = pd.read_parquet("blocked_pairs.parquet")

            # Register with the linker
            linker.table_management.register_blocked_pairs(blocked_pairs_pd)

            # Now predict() will use the registered blocked pairs
            predictions = linker.inference.predict(threshold_match_probability=0.9)
            ```
        """
        table_name_physical = "__splink__blocked_id_pairs_" + self._linker._cache_uid
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        splink_dataframe.templated_name = "__splink__blocked_id_pairs"

        self._linker._intermediate_table_cache["__splink__blocked_id_pairs"] = (
            splink_dataframe
        )
        return splink_dataframe

    def invalidate_blocked_pairs(self) -> None:
        """Invalidate the cached blocked pairs table.

        This removes the blocked pairs table from the cache, so the next call
        to `predict()` or `deterministic_link()` will recompute blocked pairs
        (unless you explicitly provide them or call `compute_blocked_pairs()`).

        You should call this method if you change the blocking rules in your
        settings and have previously cached blocked pairs.
        """
        cache = self._linker._intermediate_table_cache
        key = "__splink__blocked_id_pairs"
        if key in cache:
            splink_df = cache[key]
            try:
                splink_df.drop_table_from_database_and_remove_from_cache()
            except Exception:
                pass
            cache.pop(key, None)

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

    def invalidate_df_concat_with_tf(self) -> None:
        """Invalidate just the cached df_concat_with_tf table.

        This is a lighter-weight alternative to `invalidate_cache()` when you
        only need to recompute the concatenated input records with term
        frequency columns.

        This is useful if you've changed term frequency settings or want to
        force recomputation of this specific table.
        """
        cache = self._linker._intermediate_table_cache
        key = "__splink__df_concat_with_tf"
        if key in cache:
            splink_df = cache[key]
            try:
                splink_df.drop_table_from_database_and_remove_from_cache()
            except Exception:
                pass
            cache.pop(key, None)

    def invalidate_tf_table(self, column_name: str) -> None:
        """Invalidate the cached term frequency table for a specific column.

        This is a lighter-weight alternative to `invalidate_cache()` when you
        only need to recompute a specific term frequency lookup table.

        Args:
            column_name (str): The column name whose TF table should be
                invalidated.
        """
        input_col = InputColumn(
            column_name,
            column_info_settings=self._linker._settings_obj.column_info_settings,
            sqlglot_dialect_str=self._linker._settings_obj._sql_dialect_str,
        )
        tf_tablename = colname_to_tf_tablename(input_col)
        cache = self._linker._intermediate_table_cache
        if tf_tablename in cache:
            splink_df = cache[tf_tablename]
            try:
                splink_df.drop_table_from_database_and_remove_from_cache()
            except Exception:
                pass
            cache.pop(tf_tablename, None)

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
