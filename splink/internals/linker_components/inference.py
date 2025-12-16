from __future__ import annotations

import logging
import time
from itertools import product
from typing import TYPE_CHECKING, Any

from splink.internals.accuracy import _select_found_by_blocking_rules
from splink.internals.blocking import (
    BlockingRule,
    block_using_rules_sqls,
    compute_blocked_pairs_from_concat_with_tf,
)
from splink.internals.blocking_rule_creator import BlockingRuleCreator
from splink.internals.blocking_rule_creator_utils import to_blocking_rule_creator
from splink.internals.chunking import _blocked_pairs_cache_key
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.database_api import AcceptableInputTableType
from splink.internals.find_matches_to_new_records import (
    add_unique_id_and_source_dataset_cols_if_needed,
)
from splink.internals.misc import (
    ascii_uid,
    ensure_is_list,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import (
    predict_from_comparison_vectors_sqls_using_settings,
)
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.term_frequencies import (
    _join_new_table_to_df_concat_with_tf_sql,
    colname_to_tf_tablename,
)
from splink.internals.unique_id_concat import _composite_unique_id_from_edges_sql
from splink.internals.vertically_concatenate import (
    compute_df_concat_with_tf,
    enqueue_df_concat_with_tf,
)

if TYPE_CHECKING:
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


class LinkerInference:
    """Use your Splink model to make predictions (perform inference). Accessed via
    `linker.inference`.
    """

    def __init__(self, linker: Linker):
        self._linker = linker

    def deterministic_link(self) -> SplinkDataFrame:
        """Uses the blocking rules specified by
        `blocking_rules_to_generate_predictions` in your settings to
        generate pairwise record comparisons.

        For deterministic linkage, this should be a list of blocking rules which
        are strict enough to generate only true links.

        Deterministic linkage, however, is likely to result in missed links
        (false negatives).

        Returns:
            SplinkDataFrame: A SplinkDataFrame of the pairwise comparisons.


        Examples:

            ```py
            settings = SettingsCreator(
                link_type="dedupe_only",
                blocking_rules_to_generate_predictions=[
                    block_on("first_name", "surname"),
                    block_on("dob", "first_name"),
                ],
            )

            linker = Linker(df, settings, db_api=db_api)
            splink_df = linker.inference.deterministic_link()
            ```
        """
        pipeline = CTEPipeline()
        df_concat_with_tf = compute_df_concat_with_tf(self._linker, pipeline)
        pipeline = CTEPipeline([df_concat_with_tf])

        settings = self._linker._settings_obj

        blocked_pairs = compute_blocked_pairs_from_concat_with_tf(
            pipeline=pipeline,
            db_api=self._linker._db_api,
            splink_df_dict=self._linker._input_tables_dict,
            blocking_rules=settings._blocking_rules_to_generate_predictions,
            link_type=settings._link_type,
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        )

        pipeline = CTEPipeline([blocked_pairs, df_concat_with_tf])

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            settings._columns_to_select_for_blocking,
            ["*"],
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        deterministic_link_df = self._linker._db_api.sql_pipeline_to_splink_dataframe(
            pipeline
        )
        deterministic_link_df.metadata["is_deterministic_link"] = True

        blocked_pairs.drop_table_from_database_and_remove_from_cache()

        return deterministic_link_df

    def predict(
        self,
        threshold_match_probability: float = None,
        threshold_match_weight: float = None,
        materialise_after_computing_term_frequencies: bool = True,
        num_chunks_left: int | None = None,
        num_chunks_right: int | None = None,
    ) -> SplinkDataFrame:
        """Create a dataframe of scored pairwise comparisons using the parameters
        of the linkage model.

        Uses the blocking rules specified in the
        `blocking_rules_to_generate_predictions` key of the settings to
        generate the pairwise comparisons.

        Args:
            threshold_match_probability (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_probability above this threshold. Defaults to None.
            threshold_match_weight (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_weight above this threshold. Defaults to None.
            materialise_after_computing_term_frequencies (bool): If true, Splink
                will materialise the table containing the input nodes (rows)
                joined to any term frequencies which have been asked
                for in the settings object.  If False, this will be
                computed as part of a large CTE pipeline.   Defaults to True
            num_chunks_left (int, optional): If specified along with num_chunks_right,
                the prediction will be split into chunks and processed iteratively.
                This can help manage memory usage for large datasets.
            num_chunks_right (int, optional): If specified along with num_chunks_left,
                the prediction will be split into chunks and processed iteratively.

        Examples:
            ```py
            linker = linker(df, "saved_settings.json", db_api=db_api)
            splink_df = linker.inference.predict(threshold_match_probability=0.95)
            splink_df.as_pandas_dataframe(limit=5)

            # With chunking for large datasets
            splink_df = linker.inference.predict(
                threshold_match_probability=0.95,
                num_chunks_left=3,
                num_chunks_right=4
            )
            ```
        Returns:
            SplinkDataFrame: A SplinkDataFrame of the scored pairwise comparisons.
        """
        # Default to (1, 1) chunking if not specified
        n_left = num_chunks_left if num_chunks_left is not None else 1
        n_right = num_chunks_right if num_chunks_right is not None else 1

        # If just one chunk on each side, call predict_chunk directly
        if n_left == 1 and n_right == 1:
            return self.predict_chunk(
                left_chunk=(1, 1),
                right_chunk=(1, 1),
                threshold_match_probability=threshold_match_probability,
                threshold_match_weight=threshold_match_weight,
                materialise_after_computing_term_frequencies=materialise_after_computing_term_frequencies,
            )

        # Otherwise iterate through all chunk combinations

        chunk_results: list[SplinkDataFrame] = []

        total_chunks = n_left * n_right
        chunk_count = 0
        overall_start_time = time.time()

        for left_idx, right_idx in product(range(1, n_left + 1), range(1, n_right + 1)):
            chunk_count += 1
            logger.info(
                f"Processing chunk ({left_idx}, {n_left}) x "
                f"({right_idx}, {n_right}) "
                f"[{chunk_count}/{total_chunks}]"
            )

            chunk_result = self.predict_chunk(
                left_chunk=(left_idx, n_left),
                right_chunk=(right_idx, n_right),
                threshold_match_probability=threshold_match_probability,
                threshold_match_weight=threshold_match_weight,
                materialise_after_computing_term_frequencies=materialise_after_computing_term_frequencies,
            )
            chunk_results.append(chunk_result)

            elapsed = time.time() - overall_start_time
            percent_complete = chunk_count / total_chunks
            if percent_complete > 0:
                estimated_total = elapsed / percent_complete
                estimated_remaining = estimated_total - elapsed
                logger.info(
                    f"Completed chunk {chunk_count}/{total_chunks} "
                    f"({percent_complete:.0%}) | "
                    f"Elapsed: {elapsed:.1f}s | "
                    f"Remaining: ~{estimated_remaining:.1f}s | "
                    f"Total: ~{estimated_total:.1f}s"
                )

        overall_time = time.time() - overall_start_time
        logger.info(f"Total chunked prediction time: {overall_time:.2f} seconds")

        union_parts = [
            f"SELECT * FROM {chunk_df.physical_name}" for chunk_df in chunk_results
        ]
        union_sql = " UNION ALL ".join(union_parts)

        pipeline = CTEPipeline()
        pipeline.enqueue_sql(union_sql, "__splink__df_predict")

        combined_predictions = self._linker._db_api.sql_pipeline_to_splink_dataframe(
            pipeline
        )

        # Clean up intermediate chunk tables
        for chunk_df in chunk_results:
            chunk_df.drop_table_from_database_and_remove_from_cache()

        return combined_predictions

    def predict_chunk(
        self,
        left_chunk: tuple[int, int] | None = None,
        right_chunk: tuple[int, int] | None = None,
        threshold_match_probability: float = None,
        threshold_match_weight: float = None,
        materialise_after_computing_term_frequencies: bool = True,
    ) -> SplinkDataFrame:
        """Create a dataframe of scored pairwise comparisons for a specific chunk
        of the data.

        Args:
            left_chunk (tuple[int, int], optional): Tuple of
                (chunk_number, total_chunks) for filtering left side records.
                For example, (1, 3) means chunk 1 of 3.
            right_chunk (tuple[int, int], optional): Tuple of
                (chunk_number, total_chunks) for filtering right side records.
                For example, (2, 4) means chunk 2 of 4.
            threshold_match_probability (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_probability above this threshold. Defaults to None.
            threshold_match_weight (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_weight above this threshold. Defaults to None.
            materialise_after_computing_term_frequencies (bool): If true, Splink
                will materialise the table containing the input nodes (rows)
                joined to any term frequencies which have been asked
                for in the settings object. If False, this will be
                computed as part of a large CTE pipeline. Defaults to True

        Examples:
            ```py
            linker = linker(df, "saved_settings.json", db_api=db_api)
            # Process chunk 1 of 3 on left, chunk 2 of 4 on right
            splink_df = linker.inference.predict_chunk(
                left_chunk=(1, 3),
                right_chunk=(2, 4),
                threshold_match_probability=0.5
            )
            splink_df.as_pandas_dataframe(limit=5)
            ```
        Returns:
            SplinkDataFrame: A SplinkDataFrame of the scored pairwise comparisons
                for the specified chunk.
        """

        pipeline = CTEPipeline()
        df_concat_with_tf = compute_df_concat_with_tf(self._linker, pipeline)

        pipeline = CTEPipeline([df_concat_with_tf])

        settings = self._linker._settings_obj

        cache = self._linker._intermediate_table_cache
        cache_key = _blocked_pairs_cache_key(left_chunk, right_chunk)
        blocked_pairs_from_cache = False

        if cache_key in cache:
            blocked_pairs = cache.get_with_logging(cache_key)
            blocked_pairs_from_cache = True
            logger.info(f"Using cached blocked pairs from '{cache_key}'")
        else:
            blocked_pairs = compute_blocked_pairs_from_concat_with_tf(
                pipeline=pipeline,
                db_api=self._linker._db_api,
                splink_df_dict=self._linker._input_tables_dict,
                blocking_rules=settings._blocking_rules_to_generate_predictions,
                link_type=settings._link_type,
                source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
                unique_id_input_column=settings.column_info_settings.unique_id_input_column,
                left_chunk=left_chunk,
                right_chunk=right_chunk,
            )

        pipeline = CTEPipeline([blocked_pairs, df_concat_with_tf])

        start_time = time.time()

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            self._linker._settings_obj._columns_to_select_for_blocking,
            self._linker._settings_obj._columns_to_select_for_comparison_vector_values,
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sqls = predict_from_comparison_vectors_sqls_using_settings(
            self._linker._settings_obj,
            threshold_match_probability,
            threshold_match_weight,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        predictions = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        predict_time = time.time() - start_time
        logger.info(f"Predict time (post-blocking): {predict_time:.2f} seconds")

        if not blocked_pairs_from_cache:
            blocked_pairs.drop_table_from_database_and_remove_from_cache()

        self._linker._predict_warning()

        return predictions

    def _score_missing_cluster_edges(
        self,
        df_clusters: SplinkDataFrame,
        df_predict: SplinkDataFrame = None,
        threshold_match_probability: float = None,
        threshold_match_weight: float = None,
    ) -> SplinkDataFrame:
        """
        Given a table of clustered records, create a dataframe of scored
        pairwise comparisons for all pairs of records that belong to the same cluster.

        If you also supply a scored edges table, this will only return pairwise
        comparisons that are not already present in your scored edges table.

        Args:
            df_clusters (SplinkDataFrame): A table of clustered records, such
                as the output of
                `linker.clustering.cluster_pairwise_predictions_at_threshold()`.
                All edges within the same cluster as specified by this table will
                be scored.
                Table needs cluster_id, id columns, and any columns used in
                model comparisons.
            df_predict (SplinkDataFrame, optional): An edges table, the output of
                `linker.inference.predict()`.
                If supplied, resulting table will not include any edges already
                included in this table.
            threshold_match_probability (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_probability above this threshold. Defaults to None.
            threshold_match_weight (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_weight above this threshold. Defaults to None.

        Examples:
            ```py
            linker = linker(df, "saved_settings.json", db_api=db_api)
            df_edges = linker.inference.predict()
            df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
                df_edges,
                0.9,
            )
            df_remaining_edges = linker._score_missing_cluster_edges(
                df_clusters,
                df_edges,
            )
            df_remaining_edges.as_pandas_dataframe(limit=5)
            ```
        Returns:
            SplinkDataFrame: A SplinkDataFrame of the scored pairwise comparisons.
        """

        start_time = time.time()

        source_dataset_input_column = (
            self._linker._settings_obj.column_info_settings.source_dataset_input_column
        )
        unique_id_input_column = (
            self._linker._settings_obj.column_info_settings.unique_id_input_column
        )

        pipeline = CTEPipeline()
        enqueue_df_concat_with_tf(self._linker, pipeline)
        # we need to adjoin tf columns onto clusters table now
        # also alias cluster_id so that it doesn't interfere with existing column
        sql = f"""
        SELECT
            c.cluster_id AS _cluster_id,
            ctf.*
        FROM
            {df_clusters.physical_name} c
        LEFT JOIN
            __splink__df_concat_with_tf ctf
        ON
            c.{unique_id_input_column.name} = ctf.{unique_id_input_column.name}
        """
        if source_dataset_input_column:
            sql += (
                f" AND c.{source_dataset_input_column.name} = "
                f"ctf.{source_dataset_input_column.name}"
            )
        sqls = [
            {
                "sql": sql,
                "output_table_name": "__splink__df_clusters_renamed",
            }
        ]
        blocking_input_tablename_l = "__splink__df_clusters_renamed"
        blocking_input_tablename_r = "__splink__df_clusters_renamed"

        link_type = self._linker._settings_obj._link_type
        sqls.extend(
            block_using_rules_sqls(
                input_tablename_l=blocking_input_tablename_l,
                input_tablename_r=blocking_input_tablename_r,
                blocking_rules=[
                    BlockingRule(
                        "l._cluster_id = r._cluster_id", self._linker._sql_dialect_str
                    )
                ],
                link_type=link_type,
                source_dataset_input_column=source_dataset_input_column,
                unique_id_input_column=unique_id_input_column,
            )
        )
        # we are going to insert an intermediate table, so rename this
        sqls[-1]["output_table_name"] = "__splink__raw_blocked_id_pairs"

        sql = """
        SELECT ne.*
        FROM __splink__raw_blocked_id_pairs ne
        """
        if df_predict is not None:
            # if we are given edges, we left join them, and then keep only rows
            # where we _didn't_ have corresponding rows in edges table
            if source_dataset_input_column:
                unique_id_columns = [
                    source_dataset_input_column,
                    unique_id_input_column,
                ]
            else:
                unique_id_columns = [unique_id_input_column]
            uid_l_expr = _composite_unique_id_from_edges_sql(unique_id_columns, "l")
            uid_r_expr = _composite_unique_id_from_edges_sql(unique_id_columns, "r")
            sql_predict_with_join_keys = f"""
                SELECT *, {uid_l_expr} AS join_key_l, {uid_r_expr} AS join_key_r
                FROM {df_predict.physical_name}
            """
            sqls.append(
                {
                    "sql": sql_predict_with_join_keys,
                    "output_table_name": "__splink__df_predict_with_join_keys",
                }
            )

            sql = f"""
            {sql}
            LEFT JOIN __splink__df_predict_with_join_keys oe
            ON oe.join_key_l = ne.join_key_l AND oe.join_key_r = ne.join_key_r
            WHERE oe.join_key_l IS NULL AND oe.join_key_r IS NULL
            """

        sqls.append({"sql": sql, "output_table_name": "__splink__blocked_id_pairs"})

        pipeline.enqueue_list_of_sqls(sqls)

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            self._linker._settings_obj._columns_to_select_for_blocking,
            self._linker._settings_obj._columns_to_select_for_comparison_vector_values,
            input_tablename_l=blocking_input_tablename_l,
            input_tablename_r=blocking_input_tablename_r,
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sqls = predict_from_comparison_vectors_sqls_using_settings(
            self._linker._settings_obj,
            threshold_match_probability,
            threshold_match_weight,
        )
        sqls[-1]["output_table_name"] = "__splink__df_predict_missing_cluster_edges"
        pipeline.enqueue_list_of_sqls(sqls)

        predictions = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        predict_time = time.time() - start_time
        logger.info(f"Predict time: {predict_time:.2f} seconds")

        self._linker._predict_warning()
        return predictions

    def find_matches_to_new_records(
        self,
        records_or_tablename: AcceptableInputTableType | str,
        blocking_rules: list[BlockingRuleCreator | dict[str, Any] | str]
        | BlockingRuleCreator
        | dict[str, Any]
        | str = [],
        match_weight_threshold: float = -4,
    ) -> SplinkDataFrame:
        """Given one or more records, find records in the input dataset(s) which match
        and return in order of the Splink prediction score.

        This effectively provides a way of searching the input datasets
        for given record(s)

        Args:
            records_or_tablename (List[dict]): Input search record(s) as list of dict,
                or a table registered to the database.
            blocking_rules (list, optional): Blocking rules to select
                which records to find and score. If [], do not use a blocking
                rule - meaning the input records will be compared to all records
                provided to the linker when it was instantiated. Defaults to [].
            match_weight_threshold (int, optional): Return matches with a match weight
                above this threshold. Defaults to -4.

        Examples:
            ```py
            linker = Linker(df, "saved_settings.json", db_api=db_api)

            # You should load or pre-compute tf tables for any tables with
            # term frequency adjustments
            linker.table_management.compute_tf_table("first_name")
            # OR
            linker.table_management.register_term_frequency_lookup(df, "first_name")

            record = {'unique_id': 1,
                'first_name': "John",
                'surname': "Smith",
                'dob': "1971-05-24",
                'city': "London",
                'email': "john@smith.net"
                }
            df = linker.inference.find_matches_to_new_records(
                [record], blocking_rules=[]
            )
            ```

        Returns:
            SplinkDataFrame: The pairwise comparisons.
        """

        original_blocking_rules = (
            self._linker._settings_obj._blocking_rules_to_generate_predictions
        )
        original_link_type = self._linker._settings_obj._link_type

        blocking_rule_list: list[BlockingRuleCreator | dict[str, Any] | str] = (
            ensure_is_list(blocking_rules)
        )

        if not isinstance(records_or_tablename, str):
            uid = ascii_uid(8)
            new_records_tablename = f"__splink__df_new_records_{uid}"
            self._linker.table_management.register_table(
                records_or_tablename, new_records_tablename, overwrite=True
            )

        else:
            new_records_tablename = records_or_tablename

        new_records_df = self._linker._db_api.table_to_splink_dataframe(
            "__splink__df_new_records", new_records_tablename
        )

        pipeline = CTEPipeline()
        nodes_with_tf = compute_df_concat_with_tf(self._linker, pipeline)

        pipeline = CTEPipeline([nodes_with_tf, new_records_df])
        if len(blocking_rule_list) == 0:
            blocking_rule_list = ["1=1"]

        blocking_rules_dialected = [
            to_blocking_rule_creator(br).get_blocking_rule(
                self._linker._db_api.sql_dialect.sql_dialect_str
            )
            for br in blocking_rule_list
        ]
        for n, br in enumerate(blocking_rules_dialected):
            br.add_preceding_rules(blocking_rules_dialected[:n])

        self._linker._settings_obj._blocking_rules_to_generate_predictions = (
            blocking_rules_dialected
        )

        pipeline = add_unique_id_and_source_dataset_cols_if_needed(
            self._linker,
            new_records_df,
            pipeline,
            in_tablename="__splink__df_new_records",
            out_tablename="__splink__df_new_records_uid_fix",
        )
        settings = self._linker._settings_obj
        sqls = block_using_rules_sqls(
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_new_records_uid_fix",
            blocking_rules=blocking_rules_dialected,
            link_type="two_dataset_link_only",
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        blocked_pairs = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        pipeline = CTEPipeline([blocked_pairs, new_records_df, nodes_with_tf])

        cache = self._linker._intermediate_table_cache
        for tf_col in self._linker._settings_obj._term_frequency_columns:
            tf_table_name = colname_to_tf_tablename(tf_col)
            if tf_table_name in cache:
                tf_table = cache.get_with_logging(tf_table_name)
                pipeline.append_input_dataframe(tf_table)
            else:
                if "__splink__df_concat_with_tf" not in cache:
                    logger.warning(
                        f"No term frequencies found for column {tf_col.name}.\n"
                        "To apply term frequency adjustments, you need to register"
                        " a lookup using "
                        "`linker.table_management.register_term_frequency_lookup`."
                    )

        sql = _join_new_table_to_df_concat_with_tf_sql(
            self._linker, "__splink__df_new_records"
        )
        pipeline.enqueue_sql(sql, "__splink__df_new_records_with_tf_before_uid_fix")

        pipeline = add_unique_id_and_source_dataset_cols_if_needed(
            self._linker,
            new_records_df,
            pipeline,
            in_tablename="__splink__df_new_records_with_tf_before_uid_fix",
            out_tablename="__splink__df_new_records_with_tf",
        )

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            self._linker._settings_obj._columns_to_select_for_blocking,
            self._linker._settings_obj._columns_to_select_for_comparison_vector_values,
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_new_records_with_tf",
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        )

        pipeline.enqueue_list_of_sqls(sqls)

        sqls = predict_from_comparison_vectors_sqls_using_settings(
            self._linker._settings_obj
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sql = f"""
        select * from __splink__df_predict
        where match_weight > {match_weight_threshold}
        """

        pipeline.enqueue_sql(sql, "__splink__find_matches_predictions")

        predictions = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        self._linker._settings_obj._blocking_rules_to_generate_predictions = (
            original_blocking_rules
        )
        self._linker._settings_obj._link_type = original_link_type

        blocked_pairs.drop_table_from_database_and_remove_from_cache()

        return predictions

    def compare_two_records(
        self,
        record_1: dict[str, Any] | AcceptableInputTableType,
        record_2: dict[str, Any] | AcceptableInputTableType,
        include_found_by_blocking_rules: bool = False,
    ) -> SplinkDataFrame:
        """Use the linkage model to compare and score a pairwise record comparison
        based on the two input records provided.

        If your inputs contain multiple rows, scores for the cartesian product of
        the two inputs will be returned.

        If your inputs contain hardcoded term frequency columns (e.g.
        a tf_first_name column), then these values will be used instead of any
        provided term frequency lookup tables. or term frequency values derived
        from the input data.

        Args:
            record_1 (dict): dictionary representing the first record.  Columns names
                and data types must be the same as the columns in the settings object
            record_2 (dict): dictionary representing the second record.  Columns names
                and data types must be the same as the columns in the settings object
            include_found_by_blocking_rules (bool, optional): If True, outputs a column
                indicating whether the record pair would have been found by any of the
                blocking rules specified in
                settings.blocking_rules_to_generate_predictions. Defaults to False.

        Examples:
            ```py
            linker = Linker(df, "saved_settings.json", db_api=db_api)

            # You should load or pre-compute tf tables for any tables with
            # term frequency adjustments
            linker.table_management.compute_tf_table("first_name")
            # OR
            linker.table_management.register_term_frequency_lookup(df, "first_name")

            record_1 = {'unique_id': 1,
                'first_name': "John",
                'surname': "Smith",
                'dob': "1971-05-24",
                'city': "London",
                'email': "john@smith.net"
                }

            record_2 = {'unique_id': 1,
                'first_name': "Jon",
                'surname': "Smith",
                'dob': "1971-05-23",
                'city': "London",
                'email': "john@smith.net"
                }
            df = linker.inference.compare_two_records(record_1, record_2)

            ```

        Returns:
            SplinkDataFrame: Pairwise comparison with scored prediction
        """

        linker = self._linker

        retain_matching_columns = linker._settings_obj._retain_matching_columns
        retain_intermediate_calculation_columns = (
            linker._settings_obj._retain_intermediate_calculation_columns
        )
        linker._settings_obj._retain_matching_columns = True
        linker._settings_obj._retain_intermediate_calculation_columns = True

        cache = linker._intermediate_table_cache

        uid = ascii_uid(8)

        # Check if input is a DuckDB relation without importing DuckDB
        if isinstance(record_1, dict):
            to_register_left: AcceptableInputTableType = [record_1]
        else:
            to_register_left = record_1

        if isinstance(record_2, dict):
            to_register_right: AcceptableInputTableType = [record_2]
        else:
            to_register_right = record_2

        df_records_left = linker.table_management.register_table(
            to_register_left,
            f"__splink__compare_two_records_left_{uid}",
            overwrite=True,
        )

        df_records_left.templated_name = "__splink__compare_two_records_left"

        df_records_right = linker.table_management.register_table(
            to_register_right,
            f"__splink__compare_two_records_right_{uid}",
            overwrite=True,
        )
        df_records_right.templated_name = "__splink__compare_two_records_right"

        pipeline = CTEPipeline([df_records_left, df_records_right])

        if "__splink__df_concat_with_tf" in cache:
            nodes_with_tf = cache.get_with_logging("__splink__df_concat_with_tf")
            pipeline.append_input_dataframe(nodes_with_tf)

        tf_cols = linker._settings_obj._term_frequency_columns

        for tf_col in tf_cols:
            tf_table_name = colname_to_tf_tablename(tf_col)
            if tf_table_name in cache:
                tf_table = cache.get_with_logging(tf_table_name)
                pipeline.append_input_dataframe(tf_table)
            else:
                if "__splink__df_concat_with_tf" not in cache:
                    logger.warning(
                        f"No term frequencies found for column {tf_col.name}.\n"
                        "To apply term frequency adjustments, you need to register"
                        " a lookup using "
                        "`linker.table_management.register_term_frequency_lookup`."
                    )

        sql_join_tf = _join_new_table_to_df_concat_with_tf_sql(
            linker, "__splink__compare_two_records_left", df_records_left
        )

        pipeline.enqueue_sql(sql_join_tf, "__splink__compare_two_records_left_with_tf")

        sql_join_tf = _join_new_table_to_df_concat_with_tf_sql(
            linker, "__splink__compare_two_records_right", df_records_right
        )

        pipeline.enqueue_sql(sql_join_tf, "__splink__compare_two_records_right_with_tf")

        pipeline = add_unique_id_and_source_dataset_cols_if_needed(
            linker,
            df_records_left,
            pipeline,
            in_tablename="__splink__compare_two_records_left_with_tf",
            out_tablename="__splink__compare_two_records_left_with_tf_uid_fix",
            uid_str="_left",
        )
        pipeline = add_unique_id_and_source_dataset_cols_if_needed(
            linker,
            df_records_right,
            pipeline,
            in_tablename="__splink__compare_two_records_right_with_tf",
            out_tablename="__splink__compare_two_records_right_with_tf_uid_fix",
            uid_str="_right",
        )

        cols_to_select = self._linker._settings_obj._columns_to_select_for_blocking

        select_expr = ", ".join(cols_to_select)
        sql = f"""
        select {select_expr}, 0 as match_key
        from __splink__compare_two_records_left_with_tf_uid_fix as l
        cross join __splink__compare_two_records_right_with_tf_uid_fix as r
        """
        pipeline.enqueue_sql(sql, "__splink__compare_two_records_blocked")

        cols_to_select = (
            linker._settings_obj._columns_to_select_for_comparison_vector_values
        )
        select_expr = ", ".join(cols_to_select)
        sql = f"""
        select {select_expr}
        from __splink__compare_two_records_blocked
        """
        pipeline.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sqls_using_settings(linker._settings_obj)
        pipeline.enqueue_list_of_sqls(sqls)

        if include_found_by_blocking_rules:
            br_col = _select_found_by_blocking_rules(linker._settings_obj)
            sql = f"""
            select *, {br_col}
            from __splink__df_predict
            """

            pipeline.enqueue_sql(sql, "__splink__found_by_blocking_rules")

        predictions = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        linker._settings_obj._retain_matching_columns = retain_matching_columns
        linker._settings_obj._retain_intermediate_calculation_columns = (
            retain_intermediate_calculation_columns
        )

        return predictions
