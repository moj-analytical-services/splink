from __future__ import annotations

import logging
import time
from copy import deepcopy
from itertools import product
from typing import TYPE_CHECKING, Any, Literal, Sequence

from splink.internals.accuracy import _select_found_by_blocking_rules
from splink.internals.blocking import (
    BlockingRule,
    ExplodingBlockingRule,
    _columns_needed_for_blocking,
    block_using_rules_sqls,
    compute_blocked_pairs_from_concat_with_tf,
)
from splink.internals.blocking_analysis import _as_blocking_rule
from splink.internals.chunking import _blocked_pairs_cache_key
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.exceptions import SplinkException
from splink.internals.find_matches_to_new_records import (
    add_unique_id_and_source_dataset_cols_if_needed,
)
from splink.internals.misc import (
    ascii_uid,
    indent_sql,
    join_sql_with_union_all,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import (
    predict_from_comparison_vectors_sqls_using_settings,
)
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.splinkdataframe_utils import splink_dataframes_to_dict
from splink.internals.term_frequencies import (
    _join_new_table_to_df_concat_with_tf_sql,
    _join_tf_to_input_table_sql,
    append_term_frequencies_to_pipeline,
    colname_to_tf_tablename,
)
from splink.internals.unique_id_concat import (
    _composite_unique_id_from_edges_sql,
    _composite_unique_id_from_nodes_sql,
)
from splink.internals.vertically_concatenate import (
    enqueue_df_concat,
    enqueue_df_concat_with_tf,
    vertically_concatenate_sql,
)

if TYPE_CHECKING:
    from splink.internals.blocking_rule_creator import BlockingRuleCreator
    from splink.internals.input_column import InputColumn
    from splink.internals.linker import Linker
    from splink.internals.settings import Settings

logger = logging.getLogger(__name__)

PredictUntrainedWarningMode = Literal["auto", "always", "never"]
LinkTypeLiteralType = Literal["link_only", "link_and_dedupe", "dedupe_only"]


class LinkerInference:
    """Use your Splink model to make predictions (perform inference). Accessed via
    `linker.inference`.
    """

    def __init__(self, linker: Linker):
        self._linker = linker

    def _registered_blocked_pairs_cache_keys(self) -> list[str]:
        cache = self._linker._intermediate_table_cache
        return [
            cache_key
            for cache_key, splink_df in cache.items()
            if splink_df.metadata.get("registered_for_predict")
        ]

    @staticmethod
    def _validate_predict_warning_mode(
        warning_mode: PredictUntrainedWarningMode,
    ) -> None:
        if warning_mode not in {"auto", "always", "never"}:
            raise ValueError(
                "warning_mode must be one of 'auto', 'always' or 'never'. "
                f"Supplied value was {warning_mode!r}."
            )

    def _get_or_compute_blocked_pairs_for_predict_chunk(
        self,
        left_chunk: tuple[int, int] | None = None,
        right_chunk: tuple[int, int] | None = None,
    ) -> tuple[SplinkDataFrame, bool]:
        pipeline = CTEPipeline()
        enqueue_df_concat(self._linker, pipeline)

        settings = self._linker._settings_obj
        cache = self._linker._intermediate_table_cache
        cache_key = _blocked_pairs_cache_key(left_chunk, right_chunk)

        if cache_key in cache:
            blocked_pairs = cache.get_with_logging(cache_key)
            logger.info(f"Using cached blocked pairs from '{cache_key}'")
            return blocked_pairs, True

        blocked_pairs = compute_blocked_pairs_from_concat_with_tf(
            pipeline=pipeline,
            db_api=self._linker._db_api,
            splink_df_dict=self._linker._input_tables_dict,
            blocking_rules=settings._blocking_rules_to_generate_predictions,
            link_type=settings._link_type,
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
            df_concat_with_tf_table_name="__splink__df_concat",
            left_chunk=left_chunk,
            right_chunk=right_chunk,
        )

        cache[cache_key] = blocked_pairs
        return blocked_pairs, False

    def compute_blocked_pairs_for_predict(self) -> SplinkDataFrame:
        """Compute blocked pairs for prediction.

        Uses the blocking rules specified in the
        `blocking_rules_to_generate_predictions` key of the settings to generate
        the candidate pairs that `predict()` would score.

        This is useful when you want to materialise blocked pairs separately from
        scoring, for example to write them out and re-register them in a different
        job or on a different machine:

        ```py

        blocked_pairs = linker.inference.compute_blocked_pairs_for_predict()
        # Write the blocked pairs out to parquet (DuckDB example)
        blocked_pairs.as_duckdbpyrelation().to_parquet("blocked_pairs.parquet")

        # Then in a different session e.g. on a different machine
        blocked_pairs = db_api.register("blocked_pairs.parquet")
        linker.table_management.register_blocked_pairs_for_predict(blocked_pairs)
        predictions = linker.inference.predict()
        ```

        To compute blocked pairs for a single chunk instead, use
        `compute_blocked_pairs_for_predict_chunk()`.

        Returns:
            SplinkDataFrame: The blocked pairs table, also stored in cache.

        Examples:
            ```py
            blocked_pairs = linker.inference.compute_blocked_pairs_for_predict()
            ```
        """
        blocked_pairs, _ = self._get_or_compute_blocked_pairs_for_predict_chunk()
        return blocked_pairs

    def compute_blocked_pairs_for_predict_chunk(
        self,
        left_chunk: tuple[int, int] | None = None,
        right_chunk: tuple[int, int] | None = None,
    ) -> SplinkDataFrame:
        """Compute blocked pairs for a single chunk of the prediction.

        Uses the blocking rules specified in the
        `blocking_rules_to_generate_predictions` key of the settings to generate
        the candidate pairs for one slice of the data that `predict_chunk()` would
        score.

        This is useful when you want to materialise blocked pairs for a single
        chunk separately from scoring, for example to distribute the work across
        many machines:

        ```py

        blocked_pairs = linker.inference.compute_blocked_pairs_for_predict_chunk(
            left_chunk=(1, 3),
            right_chunk=(2, 4),
        )
        # Write the blocked pairs out to parquet (DuckDB example)
        blocked_pairs.as_duckdbpyrelation().to_parquet("blocked_pairs.parquet")

        # Then in a different session e.g. on a different machine
        blocked_pairs = db_api.register("blocked_pairs.parquet")
        linker.table_management.register_blocked_pairs_for_predict(blocked_pairs)
        predictions = linker.inference.predict()
        ```

        To compute the full blocked-pairs table in one go instead, use
        `compute_blocked_pairs_for_predict()`.

        Args:
            left_chunk: Optional tuple of (chunk_number, total_chunks) for filtering
                left side records. For example, (1, 3) means chunk 1 of 3.
            right_chunk: Optional tuple of (chunk_number, total_chunks) for filtering
                right side records. For example, (2, 4) means chunk 2 of 4.

        Returns:
            SplinkDataFrame: The blocked pairs table, also stored in cache.

        Examples:
            ```py
            linker.inference.compute_blocked_pairs_for_predict_chunk(
                left_chunk=(1, 3),
                right_chunk=(2, 4),
            )

            linker.inference.compute_blocked_pairs_for_predict_chunk(
                left_chunk=(1, 1),
                right_chunk=(1, 1),
            )
            ```
        """
        blocked_pairs, _ = self._get_or_compute_blocked_pairs_for_predict_chunk(
            left_chunk=left_chunk,
            right_chunk=right_chunk,
        )
        return blocked_pairs

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

            df = db_api.register(df, dataset_display_name="input_table")
            linker = Linker(df, settings)
            splink_df = linker.inference.deterministic_link()
            ```
        """
        pipeline = CTEPipeline()
        enqueue_df_concat(self._linker, pipeline)

        settings = self._linker._settings_obj

        blocked_pairs = compute_blocked_pairs_from_concat_with_tf(
            pipeline=pipeline,
            db_api=self._linker._db_api,
            splink_df_dict=self._linker._input_tables_dict,
            blocking_rules=settings._blocking_rules_to_generate_predictions,
            link_type=settings._link_type,
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
            df_concat_with_tf_table_name="__splink__df_concat",
        )

        pipeline = CTEPipeline([blocked_pairs])
        enqueue_df_concat_with_tf(self._linker, pipeline)

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            settings._columns_to_select_for_blocking,
            ["*"],
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
            link_type=settings._link_type,
            sql_dialect_str=self._linker._sql_dialect_str,
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
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
        num_chunks_left: int | None = None,
        num_chunks_right: int | None = None,
        warning_mode: PredictUntrainedWarningMode = "auto",
    ) -> SplinkDataFrame:
        """Create a dataframe of scored pairwise comparisons using the parameters
        of the linkage model.

        Uses the blocking rules specified in the
        `blocking_rules_to_generate_predictions` key of the settings to
        generate the pairwise comparisons.

        If blocked pairs have been manually registered using
        `linker.table_management.register_blocked_pairs_for_predict()`, this
        method scores exactly that registered table. In that workflow the
        chunking arguments (`num_chunks_left` / `num_chunks_right`) are not
        supported, because Splink cannot own chunking of a table you have
        already materialised.

        Args:
            threshold_match_probability (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_probability above this threshold. Defaults to None.
            threshold_match_weight (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_weight above this threshold. Defaults to None.
            num_chunks_left (int, optional): If specified along with num_chunks_right,
                the prediction will be split into chunks and processed iteratively.
                This can help manage memory usage for large datasets.
            num_chunks_right (int, optional): If specified along with num_chunks_left,
                the prediction will be split into chunks and processed iteratively.
            warning_mode (str, optional): Control emission of the warning shown when
                predict runs with untrained model parameters. Use "auto" to emit
                once per call to `predict()`, "always" to force emission once per
                call to `predict()`, or "never" to suppress this warning.

        Examples:
            ```py
            df = db_api.register(df, dataset_display_name="input_table")
            linker = Linker(df, "saved_settings.json")
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
        registered_blocked_pairs_cache_keys = (
            self._registered_blocked_pairs_cache_keys()
        )
        self._validate_predict_warning_mode(warning_mode)

        if registered_blocked_pairs_cache_keys and (
            num_chunks_left is not None or num_chunks_right is not None
        ):
            raise SplinkException(
                "predict(num_chunks_left=..., num_chunks_right=...) cannot be used "
                "when blocked pairs have been manually registered using "
                "linker.table_management.register_blocked_pairs_for_predict(). "
                "Splink cannot own chunking of a table you have already "
                "materialised. Call linker.inference.predict() with no chunk "
                "arguments to score the registered table."
            )

        # Default to (1, 1) chunking if not specified
        n_left = num_chunks_left if num_chunks_left is not None else 1
        n_right = num_chunks_right if num_chunks_right is not None else 1

        # If just one chunk on each side, score directly
        if n_left == 1 and n_right == 1:
            predictions = self._predict_chunk(
                left_chunk=(1, 1),
                right_chunk=(1, 1),
                threshold_match_probability=threshold_match_probability,
                threshold_match_weight=threshold_match_weight,
                warning_mode="never",
            )
            if warning_mode in {"auto", "always"}:
                self._linker._predict_warning()
            return predictions

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

            chunk_result = self._predict_chunk(
                left_chunk=(left_idx, n_left),
                right_chunk=(right_idx, n_right),
                threshold_match_probability=threshold_match_probability,
                threshold_match_weight=threshold_match_weight,
                warning_mode="never",
            )
            chunk_results.append(chunk_result)

            elapsed = time.time() - overall_start_time
            percent_complete = chunk_count / total_chunks

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
        union_sql = join_sql_with_union_all(union_parts)

        pipeline = CTEPipeline()
        pipeline.enqueue_sql(union_sql, "__splink__df_predict")

        combined_predictions = self._linker._db_api.sql_pipeline_to_splink_dataframe(
            pipeline
        )

        if warning_mode in {"auto", "always"}:
            self._linker._predict_warning()

        # Clean up intermediate chunk tables
        for chunk_df in chunk_results:
            chunk_df.drop_table_from_database_and_remove_from_cache()

        return combined_predictions

    def predict_chunk(
        self,
        left_chunk: tuple[int, int] | None = None,
        right_chunk: tuple[int, int] | None = None,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
        warning_mode: PredictUntrainedWarningMode = "auto",
    ) -> SplinkDataFrame:
        """Create a dataframe of scored pairwise comparisons for a specific chunk
        of the data.

        This method lets Splink compute and score blocking for a single slice of
        the data, for example one worker per slice in a distributed run. It is not
        supported when blocked pairs have been manually registered using
        `linker.table_management.register_blocked_pairs_for_predict()`; in that
        workflow call `linker.inference.predict()` to score the registered table.

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
            warning_mode (str, optional): Control emission of the warning shown when
                predict runs with untrained model parameters. Use "auto" to emit
                once per direct call to `predict_chunk()`, "always" to force
                emission, or "never" to suppress this warning.

        Examples:
            ```py
            df = db_api.register(df, dataset_display_name="input_table")
            linker = Linker(df, "saved_settings.json")
            # Process chunk 1 of 3 on left, chunk 2 of 4 on right
            splink_df = linker.inference.predict_chunk(
                left_chunk=(1, 3),
                right_chunk=(2, 4),
                threshold_match_probability=0.5
            )
            splink_df.as_pandas_dataframe(limit=5)

            splink_df = linker.inference.predict_chunk(
                left_chunk=(1, 1),
                right_chunk=(1, 1),
            )
            ```
        Returns:
            SplinkDataFrame: A SplinkDataFrame of the scored pairwise comparisons
                for the specified chunk.
        """
        if self._registered_blocked_pairs_cache_keys():
            raise SplinkException(
                "Blocked pairs have been manually registered; call "
                "linker.inference.predict() to score them. "
                "linker.inference.predict_chunk() is only for letting Splink "
                "compute blocking for a specific chunk."
            )

        return self._predict_chunk(
            left_chunk=left_chunk,
            right_chunk=right_chunk,
            threshold_match_probability=threshold_match_probability,
            threshold_match_weight=threshold_match_weight,
            warning_mode=warning_mode,
        )

    def _predict_chunk(
        self,
        left_chunk: tuple[int, int] | None = None,
        right_chunk: tuple[int, int] | None = None,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
        warning_mode: PredictUntrainedWarningMode = "auto",
    ) -> SplinkDataFrame:
        self._validate_predict_warning_mode(warning_mode)

        blocked_pairs, blocked_pairs_from_cache = (
            self._get_or_compute_blocked_pairs_for_predict_chunk(
                left_chunk=left_chunk,
                right_chunk=right_chunk,
            )
        )

        settings = self._linker._settings_obj

        pipeline = CTEPipeline([blocked_pairs])
        enqueue_df_concat_with_tf(self._linker, pipeline)

        start_time = time.time()

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            self._linker._settings_obj._columns_to_select_for_blocking,
            self._linker._settings_obj._columns_to_select_for_comparison_vector_values,
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
            link_type=settings._link_type,
            sql_dialect_str=self._linker._sql_dialect_str,
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

        if warning_mode in {"auto", "always"}:
            self._linker._predict_warning()

        return predictions

    def _score_missing_cluster_edges(
        self,
        df_clusters: SplinkDataFrame,
        df_predict: SplinkDataFrame | None = None,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
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
            df = db_api.register(df, dataset_display_name="input_table")
            linker = Linker(df, "saved_settings.json")
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

    def score_pair(
        self,
        record_left: dict[str, Any] | SplinkDataFrame,
        record_right: dict[str, Any] | SplinkDataFrame,
        include_found_by_blocking_rules: bool = False,
    ) -> SplinkDataFrame:
        """Use the linkage model to score a single pairwise record comparison.

        Each input may be a Python ``dict`` representing a single record, or a
        ``SplinkDataFrame``.

        The usual usage is to provide any required term frequency values directly
        in the input records as hardcoded term frequency columns (e.g.
        a tf_first_name column). If these values are not provided, Splink falls
        back to any registered term frequency lookup tables, or term frequency values
        derived from the input data.

        Args:
            record_left (dict | SplinkDataFrame): the left-hand record.  Column names
                and data types must be the same as the columns in the settings object
            record_right (dict | SplinkDataFrame): the right-hand record.  Column
                names and data types must be the same as the columns in the settings
                object
            include_found_by_blocking_rules (bool, optional): If True, outputs a column
                indicating whether the record pair would have been found by any of the
                blocking rules specified in
                settings.blocking_rules_to_generate_predictions. Defaults to False.

        Examples:
            ```py
            df = db_api.register(df, dataset_display_name="input_table")
            linker = Linker(df, "saved_settings.json")

            # If you do not provide tf values in the records, you should load or
            # pre-compute tf tables for any columns with term frequency adjustments
            linker.table_management.compute_tf_table("first_name")
            # OR
            linker.table_management.register_term_frequency_lookup(tf, "first_name")

            record_1 = {'unique_id': 1,
                'first_name': "John",
                'surname': "Smith",
                'dob': "1971-05-24",
                'city': "London",
                'email': "john@smith.net",
                'tf_first_name': 0.001,
                }

            record_2 = {'unique_id': 1,
                'first_name': "Jon",
                'surname': "Smith",
                'dob': "1971-05-23",
                'city': "London",
                'email': "john@smith.net",
                'tf_first_name': 0.0005,
                }
            df = linker.inference.score_pair(record_1, record_2)

            ```

        Returns:
            SplinkDataFrame: Pairwise comparison with scored prediction
        """
        return self._score_pairs_core(
            self._normalise_score_pair_input(record_left, "record_left"),
            self._normalise_score_pair_input(record_right, "record_right"),
            include_found_by_blocking_rules,
        )

    def score_pairs(
        self,
        records_left: list[dict[str, Any]] | SplinkDataFrame,
        records_right: list[dict[str, Any]] | SplinkDataFrame,
        include_found_by_blocking_rules: bool = False,
    ) -> SplinkDataFrame:
        """Use the linkage model to score pairwise record comparisons formed from
        the cartesian product of the two inputs provided.

        No blocking rules are applied: every record in ``records_left`` is compared
        against every record in ``records_right``. To score a single known pair use
        ``score_pair``; to generate candidate pairs using blocking rules, use
        ``predict_within`` or ``predict_between`` instead.

        Each input may be a list of Python ``dict``s representing records, or a
        ``SplinkDataFrame``.

        The usual usage is to provide any required term frequency values directly
        in the input records as hardcoded term frequency columns (e.g.
        a tf_first_name column). If these values are not provided, Splink falls
        back to any registered term frequency lookup tables, or term frequency values
        derived from the input data.

        Args:
            records_left (list[dict] | SplinkDataFrame): the left-hand records.
                Column names and data types must be the same as the columns in the
                settings object
            records_right (list[dict] | SplinkDataFrame): the right-hand records.
                Column names and data types must be the same as the columns in the
                settings object
            include_found_by_blocking_rules (bool, optional): If True, outputs a column
                indicating whether the record pair would have been found by any of the
                blocking rules specified in
                settings.blocking_rules_to_generate_predictions. Defaults to False.

        Examples:
            ```py
            df = db_api.register(df, dataset_display_name="input_table")
            linker = Linker(df, "saved_settings.json")

            linker.table_management.compute_tf_table("first_name")

            df = linker.inference.score_pairs(records_left, records_right)
            ```

        Returns:
            SplinkDataFrame: Pairwise comparisons with scored predictions
        """
        return self._score_pairs_core(
            self._normalise_score_pairs_input(records_left, "records_left"),
            self._normalise_score_pairs_input(records_right, "records_right"),
            include_found_by_blocking_rules,
        )

    @staticmethod
    def _normalise_score_pair_input(
        record: dict[str, Any] | SplinkDataFrame, arg_name: str
    ) -> list[dict[str, Any]] | SplinkDataFrame:
        if isinstance(record, SplinkDataFrame):
            return record
        if isinstance(record, dict):
            return [record]
        raise TypeError(
            f"{arg_name} must be a dict or a SplinkDataFrame, "
            f"got {type(record).__name__}."
        )

    @staticmethod
    def _normalise_score_pairs_input(
        records: list[dict[str, Any]] | SplinkDataFrame, arg_name: str
    ) -> list[dict[str, Any]] | SplinkDataFrame:
        if isinstance(records, SplinkDataFrame):
            return records
        if isinstance(records, list):
            return records
        raise TypeError(
            f"{arg_name} must be a list of dicts or a SplinkDataFrame, "
            f"got {type(records).__name__}."
        )

    def _prepare_score_input(
        self,
        records: list[dict[str, Any]] | SplinkDataFrame,
        templated_name: str,
        uid: str,
    ) -> SplinkDataFrame:
        """Normalise a ``score_pair`` / ``score_pairs`` input into a SplinkDataFrame
        with a fixed templated name expected by the scoring pipeline.

        A user-supplied ``SplinkDataFrame`` is never mutated: a fresh reference to
        the same physical table is returned instead.
        """
        linker = self._linker
        if isinstance(records, SplinkDataFrame):
            return linker._db_api.table_to_splink_dataframe(
                templated_name, records.physical_name
            )
        df = linker.table_management.register_table(
            records,
            f"{templated_name}_{uid}",
            overwrite=True,
        )
        df.templated_name = templated_name
        return df

    def _score_pairs_core(
        self,
        records_left: list[dict[str, Any]] | SplinkDataFrame,
        records_right: list[dict[str, Any]] | SplinkDataFrame,
        include_found_by_blocking_rules: bool,
    ) -> SplinkDataFrame:
        linker = self._linker

        retain_matching_columns = linker._settings_obj._retain_matching_columns
        retain_intermediate_calculation_columns = (
            linker._settings_obj._retain_intermediate_calculation_columns
        )
        linker._settings_obj._retain_matching_columns = True
        linker._settings_obj._retain_intermediate_calculation_columns = True

        uid = ascii_uid(8)

        df_records_left = self._prepare_score_input(
            records_left, "__splink__compare_two_records_left", uid
        )
        df_records_right = self._prepare_score_input(
            records_right, "__splink__compare_two_records_right", uid
        )

        pipeline = CTEPipeline([df_records_left, df_records_right])
        append_term_frequencies_to_pipeline(linker, pipeline)

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

    # ------------------------------------------------------------------
    # Blocked prediction over supplied records (predict_within /
    # predict_between) and their shared helpers
    # ------------------------------------------------------------------

    def _clone_settings_with_overrides(
        self,
        link_type: LinkTypeLiteralType | None,
        blocking_rules_to_generate_predictions: (
            list[BlockingRuleCreator | BlockingRule | str | dict[str, Any]] | None
        ),
    ) -> "Settings":
        """Return a deep copy of the trained settings with optional overrides of
        ``link_type`` and ``blocking_rules_to_generate_predictions`` applied.

        The live settings object is never mutated.
        """
        settings = deepcopy(self._linker._settings_obj)
        if link_type is not None:
            settings._link_type = link_type
        if blocking_rules_to_generate_predictions is not None:
            brs = [
                _as_blocking_rule(br, settings._sql_dialect_str)
                for br in blocking_rules_to_generate_predictions
            ]
            settings._blocking_rules_to_generate_predictions = (
                BlockingRule._add_preceding_rules_to_each_blocking_rule(brs)
            )
        return settings

    def _require_registered_term_frequencies(
        self, dfs: list[SplinkDataFrame]
    ) -> list[SplinkDataFrame]:
        """Return the registered term-frequency tables required to score the
        supplied records.

        Unlike ``predict()``, the ``predict_within`` / ``predict_between``
        primitives do not derive term-frequency values from the supplied data. If a
        term-frequency adjustment is required for a column and it is neither present
        as a hardcoded ``tf_*`` column on the supplied records nor available as a
        registered term-frequency table, a ``SplinkException`` is raised.
        """
        settings = self._linker._settings_obj
        cache = self._linker._intermediate_table_cache

        supplied_columns: list[Any] = []
        for df in dfs:
            supplied_columns.extend(df.columns)

        tf_tables: list[SplinkDataFrame] = []
        missing: list[str] = []
        for tf_col in settings._term_frequency_columns:
            tf_input_column = settings._input_column(tf_col.tf_name)
            if tf_input_column in supplied_columns:
                # tf value supplied directly as a hardcoded tf_* column
                continue
            tf_table_name = colname_to_tf_tablename(tf_col)
            if tf_table_name in cache:
                tf_tables.append(cache.get_with_logging(tf_table_name))
            else:
                missing.append(tf_col.name)

        if missing:
            raise SplinkException(
                "predict_within / predict_between require term-frequency tables to "
                "be registered (or tf_* columns to be present on the supplied "
                f"records). Missing term-frequency information for column(s): "
                f"{missing}. Register them with "
                "linker.table_management.compute_tf_table(col) or "
                "linker.table_management.register_term_frequency_lookup(...), or "
                "include hardcoded tf_<col> columns on the supplied records."
            )
        return tf_tables

    def _concat_sql_for_dict(
        self,
        splink_df_dict: dict[str, SplinkDataFrame],
        force_source_dataset: bool,
    ) -> str:
        """Build the vertical-concatenation SQL for a supplied collection.

        When ``force_source_dataset`` is True a ``source_dataset`` column is added
        for every input table (even when only a single table is supplied). This is
        needed by ``predict_between`` so that ``link_only`` source-dataset semantics
        and composite unique ids are well defined on each side.
        """
        settings = self._linker._settings_obj
        sds_input_column = settings.column_info_settings.source_dataset_input_column

        if not force_source_dataset:
            return vertically_concatenate_sql(splink_df_dict, sds_input_column)

        df_first = next(iter(splink_df_dict.values()))
        columns = df_first.columns_escaped
        source_dataset_already_exists = (
            sds_input_column is not None and sds_input_column in df_first.columns
        )

        sqls = []
        for df_obj in splink_df_dict.values():
            select_expressions = list(columns)
            if not source_dataset_already_exists:
                select_expressions.insert(
                    0, f"'{df_obj.dataset_display_name}' as source_dataset"
                )
            select_cols_sql = ",\n".join(
                indent_sql(expr) for expr in select_expressions
            )
            sqls.append(
                f"""
                select
{select_cols_sql}
                from {df_obj.physical_name}
                """
            )
        return join_sql_with_union_all(sqls)

    def _enqueue_concat_with_tf_cte(
        self,
        pipeline: CTEPipeline,
        splink_df_dict: dict[str, SplinkDataFrame],
        output_table_name: str,
        force_source_dataset: bool,
    ) -> None:
        """Enqueue a concat-with-tf CTE over a supplied collection.

        Term-frequency tables must already have been appended to the pipeline as
        input dataframes (see ``_require_registered_term_frequencies``).
        """
        pre_tf_name = f"{output_table_name}__pre_tf"
        pipeline.enqueue_sql(
            self._concat_sql_for_dict(splink_df_dict, force_source_dataset),
            pre_tf_name,
        )
        pipeline.enqueue_sql(
            _join_tf_to_input_table_sql(self._linker, pre_tf_name),
            output_table_name,
        )

    def predict_within(
        self,
        splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
        *,
        link_type: LinkTypeLiteralType | None = None,
        blocking_rules_to_generate_predictions: (
            list[BlockingRuleCreator | BlockingRule | str | dict[str, Any]] | None
        ) = None,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
        warning_mode: PredictUntrainedWarningMode = "auto",
    ) -> SplinkDataFrame:
        """Generate blocked, scored pairwise predictions within a
        supplied collection of records, using the trained model.

        The input shape mirrors the ``Linker`` constructor: pass a single
        ``SplinkDataFrame`` for ``dedupe_only``, or a list of
        ``SplinkDataFrame``s for ``link_only`` and ``link_and_dedupe``. Candidate pairs
        are generated using blocking rules (the trained rules by default), respecting
        the model's ``link_type``, and scored.

        Unlike ``predict()`` this does not derive term-frequency values from the
        supplied data: any required term-frequency tables must be registered (or
        supplied as hardcoded ``tf_*`` columns), otherwise a ``SplinkException`` is
        raised.

        Args:
            splink_dataframe_or_dataframes: A single ``SplinkDataFrame`` or a
                list of them, registered against the same db_api.
            link_type: Optionally override the trained ``link_type``.
            blocking_rules_to_generate_predictions: Optionally override the blocking
                rules used to generate candidate pairs.
            threshold_match_probability: If specified, only return pairs with a
                match probability above this threshold.
            threshold_match_weight: If specified, only return pairs with a match
                weight above this threshold.
            warning_mode: Control emission of the untrained-model warning.

        Returns:
            SplinkDataFrame: A SplinkDataFrame of the scored pairwise comparisons.
        """
        self._validate_predict_warning_mode(warning_mode)
        settings = self._clone_settings_with_overrides(
            link_type, blocking_rules_to_generate_predictions
        )
        splink_df_dict = splink_dataframes_to_dict(splink_dataframe_or_dataframes)

        source_dataset_input_column = (
            settings.column_info_settings.source_dataset_input_column
        )
        unique_id_input_column = settings.column_info_settings.unique_id_input_column

        # Stage 1: blocking. Blocking does not need term-frequency values, so use a
        # plain concat (which also carries source_dataset for multi-table inputs).
        blocking_pipeline = CTEPipeline()
        blocking_pipeline.enqueue_sql(
            self._concat_sql_for_dict(splink_df_dict, force_source_dataset=False),
            "__splink__df_concat",
        )
        blocked_pairs = compute_blocked_pairs_from_concat_with_tf(
            pipeline=blocking_pipeline,
            db_api=self._linker._db_api,
            splink_df_dict=splink_df_dict,
            blocking_rules=settings._blocking_rules_to_generate_predictions,
            link_type=settings._link_type,
            source_dataset_input_column=source_dataset_input_column,
            unique_id_input_column=unique_id_input_column,
            df_concat_with_tf_table_name="__splink__df_concat",
        )

        # Stage 2: build comparison vectors and score.
        pipeline = CTEPipeline([blocked_pairs])
        self._append_required_tf_to_pipeline(pipeline, list(splink_df_dict.values()))
        self._enqueue_concat_with_tf_cte(
            pipeline,
            splink_df_dict,
            "__splink__df_concat_with_tf",
            force_source_dataset=False,
        )

        predictions = self._score_blocked_pairs(
            pipeline=pipeline,
            settings=settings,
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            threshold_match_probability=threshold_match_probability,
            threshold_match_weight=threshold_match_weight,
        )

        blocked_pairs.drop_table_from_database_and_remove_from_cache()

        if warning_mode in {"auto", "always"}:
            self._linker._predict_warning()

        return predictions

    def predict_between(
        self,
        left: SplinkDataFrame | Sequence[SplinkDataFrame],
        right: SplinkDataFrame | Sequence[SplinkDataFrame],
        *,
        link_type: LinkTypeLiteralType | None = None,
        blocking_rules_to_generate_predictions: (
            list[BlockingRuleCreator | BlockingRule | str | dict[str, Any]] | None
        ) = None,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
        warning_mode: PredictUntrainedWarningMode = "auto",
    ) -> SplinkDataFrame:
        """Generate blocked, scored pairwise predictions *between*
        two supplied collections of records, using the trained model.

        Candidate pairs are generated between records in the left dataset(s)
        and right dataset(s) respectively, never within them.
        The trained model's ``link_type`` source
        condition is then also applied, e.g. for ``link_only``, pairs are additionally
        required to come from different source datasets

        One key use case is incremental record linkage, in which we want
        to find links between the existing and new records, but not within
        the existing records.

        Often, in addition, you'd want to find links within the new records,
        for which you'd need to also use predict_within().

        Note that ``left`` and ``right`` are *roles* (for example existing vs new),
        not source datasets

        Term-frequency tables must be registered (or
        supplied as hardcoded ``tf_*`` columns), otherwise a ``SplinkException`` is
        raised.

        Args:
            left: The left-hand (role) collection: a single ``SplinkDataFrame`` or a
                sequence of them.
            right: The right-hand (role) collection: a single ``SplinkDataFrame`` or
                a sequence of them.
            link_type: Optionally override the trained ``link_type``.
            blocking_rules_to_generate_predictions: Optionally override the blocking
                rules used to generate candidate pairs.
            threshold_match_probability: If specified, only return pairs with a
                match probability above this threshold.
            threshold_match_weight: If specified, only return pairs with a match
                weight above this threshold.
            warning_mode: Control emission of the untrained-model warning.

        Returns:
            SplinkDataFrame: A SplinkDataFrame of the scored pairwise comparisons.
        """
        self._validate_predict_warning_mode(warning_mode)
        settings = self._clone_settings_with_overrides(
            link_type, blocking_rules_to_generate_predictions
        )

        left_dict = splink_dataframes_to_dict(left)
        right_dict = splink_dataframes_to_dict(right)
        all_dfs = list(left_dict.values()) + list(right_dict.values())

        source_dataset_input_column = (
            settings.column_info_settings.source_dataset_input_column
        )
        unique_id_input_column = settings.column_info_settings.unique_id_input_column
        # link_only needs a source_dataset column on each side both for the source
        # condition and for unambiguous composite unique ids.
        force_source_dataset = source_dataset_input_column is not None

        # Exploding blocking rules require their marginal exploded id-pair tables to
        # be materialised before blocking (block_using_rules_sqls reads from them).
        exploding_blocking_rules = [
            br
            for br in settings._blocking_rules_to_generate_predictions
            if isinstance(br, ExplodingBlockingRule)
        ]
        if exploding_blocking_rules:
            self._materialise_exploded_id_tables_for_predict_between(
                exploding_blocking_rules=exploding_blocking_rules,
                left_dict=left_dict,
                right_dict=right_dict,
                source_dataset_input_column=source_dataset_input_column,
                unique_id_input_column=unique_id_input_column,
                force_source_dataset=force_source_dataset,
            )

        blocking_left_table_name = "__splink__df_concat_left"
        blocking_right_table_name = "__splink__df_concat_right"
        left_table_name = "__splink__df_concat_with_tf_left"
        right_table_name = "__splink__df_concat_with_tf_right"

        # Stage 1: build each side's plain concat and block left x right.
        blocking_pipeline = CTEPipeline()
        blocking_pipeline.enqueue_sql(
            self._concat_sql_for_dict(left_dict, force_source_dataset),
            blocking_left_table_name,
        )
        blocking_pipeline.enqueue_sql(
            self._concat_sql_for_dict(right_dict, force_source_dataset),
            blocking_right_table_name,
        )

        # the correctly blocking is achieve using a nice trick:
        # `two_dataset_link_only`` ensures only between-dataset links.
        # because it `inner join` between the left and right tables
        # rather than concat and self join.
        block_sqls = block_using_rules_sqls(
            input_tablename_l=blocking_left_table_name,
            input_tablename_r=blocking_right_table_name,
            blocking_rules=settings._blocking_rules_to_generate_predictions,
            link_type="two_dataset_link_only",
            source_dataset_input_column=source_dataset_input_column,
            unique_id_input_column=unique_id_input_column,
        )
        # block_using_rules_sqls outputs __splink__blocked_id_pairs as its last step.
        # For link_only, additionally require the pair to come from different source
        # datasets. dedupe_only / link_and_dedupe keep all left x right pairs.
        apply_source_filter = (
            settings._link_type == "link_only"
            and source_dataset_input_column is not None
        )
        if apply_source_filter:
            # Apply the link_only source condition as a post-blocking filter.
            block_sqls[-1]["output_table_name"] = (
                "__splink__blocked_id_pairs_unfiltered"
            )
            blocking_pipeline.enqueue_list_of_sqls(block_sqls)

            unique_id_columns = [source_dataset_input_column, unique_id_input_column]
            uid_l_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "l")
            uid_r_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "r")
            sds_col = source_dataset_input_column.name
            filter_sql = f"""
            select b.match_key, b.join_key_l, b.join_key_r
            from __splink__blocked_id_pairs_unfiltered as b
            inner join {blocking_left_table_name} as l
                on {uid_l_expr} = b.join_key_l
            inner join {blocking_right_table_name} as r
                on {uid_r_expr} = b.join_key_r
            where l.{sds_col} != r.{sds_col}
            """
            blocking_pipeline.enqueue_sql(filter_sql, "__splink__blocked_id_pairs")
        else:
            blocking_pipeline.enqueue_list_of_sqls(block_sqls)

        blocked_pairs = self._linker._db_api.sql_pipeline_to_splink_dataframe(
            blocking_pipeline
        )

        # The materialised exploded id-pair tables have now been consumed by blocking.
        for br in exploding_blocking_rules:
            br.drop_materialised_id_pairs_dataframe()

        # Stage 2: rebuild each side's concat-with-tf and score.
        pipeline = CTEPipeline([blocked_pairs])
        self._append_required_tf_to_pipeline(pipeline, all_dfs)
        self._enqueue_concat_with_tf_cte(
            pipeline, left_dict, left_table_name, force_source_dataset
        )
        self._enqueue_concat_with_tf_cte(
            pipeline, right_dict, right_table_name, force_source_dataset
        )

        predictions = self._score_blocked_pairs(
            pipeline=pipeline,
            settings=settings,
            input_tablename_l=left_table_name,
            input_tablename_r=right_table_name,
            threshold_match_probability=threshold_match_probability,
            threshold_match_weight=threshold_match_weight,
        )

        blocked_pairs.drop_table_from_database_and_remove_from_cache()

        if warning_mode in {"auto", "always"}:
            self._linker._predict_warning()

        return predictions

    def _materialise_exploded_id_tables_for_predict_between(
        self,
        *,
        exploding_blocking_rules: list[ExplodingBlockingRule],
        left_dict: dict[str, SplinkDataFrame],
        right_dict: dict[str, SplinkDataFrame],
        source_dataset_input_column: "InputColumn | None",
        unique_id_input_column: "InputColumn",
        force_source_dataset: bool,
    ) -> None:
        """Materialise the marginal exploded id-pair tables for the role-split
        (left x right) blocking used by ``predict_between``.

        Unlike ``materialise_exploded_id_tables`` (which derives its two sides from a
        single concat split on ``source_dataset``), ``predict_between`` blocks two
        independent role collections, so each side is exploded from its own concat.
        Pairs are generated with ``where 1=1`` semantics; the ``link_only`` source
        condition is applied later as a post-blocking filter.
        """
        db_api = self._linker._db_api
        for br in exploding_blocking_rules:
            pipeline = CTEPipeline()

            left_concat = "__splink__predict_between_explode_concat_left"
            right_concat = "__splink__predict_between_explode_concat_right"
            pipeline.enqueue_sql(
                self._concat_sql_for_dict(left_dict, force_source_dataset),
                left_concat,
            )
            pipeline.enqueue_sql(
                self._concat_sql_for_dict(right_dict, force_source_dataset),
                right_concat,
            )

            input_columns = _columns_needed_for_blocking(
                [*br.preceding_rules, br],
                source_dataset_input_column=source_dataset_input_column,
                unique_id_input_column=unique_id_input_column,
            )
            arrays_to_explode_cols = [
                br._input_column(colname) for colname in br.array_columns_to_explode
            ]
            other_cols = [
                col for col in input_columns if col not in arrays_to_explode_cols
            ]

            left_unnested = "__splink__predict_between_left_unnested"
            right_unnested = "__splink__predict_between_right_unnested"
            pipeline.enqueue_sql(
                db_api.sql_dialect.explode_arrays_sql(
                    left_concat,
                    br.array_columns_to_explode,
                    [col.name for col in other_cols],
                ),
                left_unnested,
            )
            pipeline.enqueue_sql(
                db_api.sql_dialect.explode_arrays_sql(
                    right_concat,
                    br.array_columns_to_explode,
                    [col.name for col in other_cols],
                ),
                right_unnested,
            )

            marginal_sql = br.marginal_exploded_id_pairs_table_sql(
                source_dataset_input_column=source_dataset_input_column,
                unique_id_input_column=unique_id_input_column,
                br=br,
                link_type="two_dataset_link_only",
                input_tablename_l=left_unnested,
                input_tablename_r=right_unnested,
            )
            table_name = (
                f"__splink__predict_between_marginal_exploded_ids_mk_{br.match_key}"
            )
            pipeline.enqueue_sql(marginal_sql, table_name)
            br.exploded_id_pair_table = db_api.sql_pipeline_to_splink_dataframe(
                pipeline
            )

    def _append_required_tf_to_pipeline(
        self, pipeline: CTEPipeline, dfs: list[SplinkDataFrame]
    ) -> None:
        for tf_table in self._require_registered_term_frequencies(dfs):
            pipeline.append_input_dataframe(tf_table)

    def _score_blocked_pairs(
        self,
        *,
        pipeline: CTEPipeline,
        settings: "Settings",
        input_tablename_l: str,
        input_tablename_r: str,
        threshold_match_probability: float | None,
        threshold_match_weight: float | None,
    ) -> SplinkDataFrame:
        """Compute comparison vectors and scored predictions from a materialised
        ``__splink__blocked_id_pairs`` table (already added to ``pipeline``)."""
        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            settings._columns_to_select_for_blocking,
            settings._columns_to_select_for_comparison_vector_values,
            input_tablename_l=input_tablename_l,
            input_tablename_r=input_tablename_r,
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
            link_type=settings._link_type,
            sql_dialect_str=self._linker._sql_dialect_str,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sqls = predict_from_comparison_vectors_sqls_using_settings(
            settings,
            threshold_match_probability,
            threshold_match_weight,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        return self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
