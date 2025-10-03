from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Iterable, Literal

from splink.internals.accuracy import _select_found_by_blocking_rules
from splink.internals.blocking import (
    BlockingRule,
    block_using_rules_sqls,
    blocking_rule_to_obj,
    materialise_exploded_id_tables,
)
from splink.internals.blocking_rule_creator import BlockingRuleCreator
from splink.internals.blocking_rule_creator_utils import to_blocking_rule_creator
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.database_api import AcceptableInputTableType
from splink.internals.find_matches_to_new_records import (
    add_unique_id_and_source_dataset_cols_if_needed,
)
from splink.internals.exceptions import SplinkException
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
    split_df_concat_with_tf_into_two_tables_sqls,
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
        # Allows clustering during a deterministic linkage.
        # This is used in `cluster_pairwise_predictions_at_threshold`
        # to set the cluster threshold to 1

        df_concat_with_tf = compute_df_concat_with_tf(self._linker, pipeline)
        pipeline = CTEPipeline([df_concat_with_tf])
        link_type = self._linker._settings_obj._link_type

        blocking_input_tablename_l = "__splink__df_concat_with_tf"
        blocking_input_tablename_r = "__splink__df_concat_with_tf"

        link_type = self._linker._settings_obj._link_type
        if (
            len(self._linker._input_tables_dict) == 2
            and self._linker._settings_obj._link_type == "link_only"
        ):
            sqls = split_df_concat_with_tf_into_two_tables_sqls(
                "__splink__df_concat_with_tf",
                self._linker._settings_obj.column_info_settings.source_dataset_column_name,
            )
            pipeline.enqueue_list_of_sqls(sqls)

            blocking_input_tablename_l = "__splink__df_concat_with_tf_left"
            blocking_input_tablename_r = "__splink__df_concat_with_tf_right"
            link_type = "two_dataset_link_only"

        exploding_br_with_id_tables = materialise_exploded_id_tables(
            link_type=link_type,
            blocking_rules=self._linker._settings_obj._blocking_rules_to_generate_predictions,
            db_api=self._linker._db_api,
            splink_df_dict=self._linker._input_tables_dict,
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
        )

        sqls = block_using_rules_sqls(
            input_tablename_l=blocking_input_tablename_l,
            input_tablename_r=blocking_input_tablename_r,
            blocking_rules=self._linker._settings_obj._blocking_rules_to_generate_predictions,
            link_type=link_type,
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)
        blocked_pairs = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        pipeline = CTEPipeline([blocked_pairs, df_concat_with_tf])

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            self._linker._settings_obj._columns_to_select_for_blocking,
            ["*"],
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        deterministic_link_df = self._linker._db_api.sql_pipeline_to_splink_dataframe(
            pipeline
        )
        deterministic_link_df.metadata["is_deterministic_link"] = True

        [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]
        blocked_pairs.drop_table_from_database_and_remove_from_cache()

        return deterministic_link_df

    def predict(
        self,
        threshold_match_probability: float = None,
        threshold_match_weight: float = None,
        materialise_after_computing_term_frequencies: bool = True,
        materialise_blocked_pairs: bool = True,
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
            materialise_blocked_pairs: In the blocking phase, materialise the table
                of pairs of records that will be scored

        Examples:
            ```py
            linker = linker(df, "saved_settings.json", db_api=db_api)
            splink_df = linker.inference.predict(threshold_match_probability=0.95)
            splink_df.as_pandas_dataframe(limit=5)
            ```
        Returns:
            SplinkDataFrame: A SplinkDataFrame of the scored pairwise comparisons.
        """

        pipeline = CTEPipeline()

        # If materialise_after_computing_term_frequencies=False and the user only
        # calls predict, it runs as a single pipeline with no materialisation
        # of anything.

        # In duckdb, calls to random() in a CTE pipeline cause problems:
        # https://gist.github.com/RobinL/d329e7004998503ce91b68479aa41139
        if (
            materialise_after_computing_term_frequencies
            or self._linker._sql_dialect.sql_dialect_str == "duckdb"
        ):
            df_concat_with_tf = compute_df_concat_with_tf(self._linker, pipeline)
            pipeline = CTEPipeline([df_concat_with_tf])
        else:
            pipeline = enqueue_df_concat_with_tf(self._linker, pipeline)

        start_time = time.time()

        blocking_input_tablename_l = "__splink__df_concat_with_tf"
        blocking_input_tablename_r = "__splink__df_concat_with_tf"

        link_type = self._linker._settings_obj._link_type
        if (
            len(self._linker._input_tables_dict) == 2
            and self._linker._settings_obj._link_type == "link_only"
        ):
            sqls = split_df_concat_with_tf_into_two_tables_sqls(
                "__splink__df_concat_with_tf",
                self._linker._settings_obj.column_info_settings.source_dataset_column_name,
            )
            pipeline.enqueue_list_of_sqls(sqls)

            blocking_input_tablename_l = "__splink__df_concat_with_tf_left"
            blocking_input_tablename_r = "__splink__df_concat_with_tf_right"
            link_type = "two_dataset_link_only"

        # If exploded blocking rules exist, we need to materialise
        # the tables of ID pairs

        exploding_br_with_id_tables = materialise_exploded_id_tables(
            link_type=link_type,
            blocking_rules=self._linker._settings_obj._blocking_rules_to_generate_predictions,
            db_api=self._linker._db_api,
            splink_df_dict=self._linker._input_tables_dict,
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
        )

        sqls = block_using_rules_sqls(
            input_tablename_l=blocking_input_tablename_l,
            input_tablename_r=blocking_input_tablename_r,
            blocking_rules=self._linker._settings_obj._blocking_rules_to_generate_predictions,
            link_type=link_type,
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
        )

        pipeline.enqueue_list_of_sqls(sqls)

        if materialise_blocked_pairs:
            blocked_pairs = self._linker._db_api.sql_pipeline_to_splink_dataframe(
                pipeline
            )

            pipeline = CTEPipeline([blocked_pairs, df_concat_with_tf])
            blocking_time = time.time() - start_time
            logger.info(f"Blocking time: {blocking_time:.2f} seconds")
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
            sql_infinity_expression=self._linker._infinity_expression,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        predictions = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        predict_time = time.time() - start_time
        logger.info(f"Predict time: {predict_time:.2f} seconds")

        self._linker._predict_warning()

        [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]
        if materialise_blocked_pairs:
            blocked_pairs.drop_table_from_database_and_remove_from_cache()

        return predictions


    def predict_sharded(
        self,
        num_shards: int,
        threshold_match_probability: float | None = None,
        threshold_match_weight: float | None = None,
        materialise_after_computing_term_frequencies: bool = True,
        materialise_blocked_pairs: bool = True,
        shard_side: str | None = "auto",
    ) -> SplinkDataFrame:
        """Run inference by splitting the data into shards to reduce memory usage.

        Args:
            num_shards (int): Number of shards to split the data into. Must be
                between 2 and 50 (inclusive).
            threshold_match_probability (float, optional): Passed through to
                :meth:`predict`. Defaults to None.
            threshold_match_weight (float, optional): Passed through to
                :meth:`predict`. Defaults to None.
            materialise_after_computing_term_frequencies (bool): Passed through
                to :meth:`predict`. Defaults to True.
            materialise_blocked_pairs (bool): Passed through to :meth:`predict`.
                Defaults to True.
            shard_side (str | None): Which side of the linkage to shard. Accepts
                "left", "right", or "auto". When "auto", dedupe jobs shard the
                single logical left table, while link jobs shard the larger side
                when one table has at least 10% more rows; ties shard the left side.

        Returns:
            SplinkDataFrame: Combined predictions identical to those generated by
            :meth:`predict` but computed in memory-efficient shards.
        """
        run_identifier = ascii_uid(8)

        shard_side = "auto" if shard_side is None else shard_side.lower()
        if shard_side not in {"auto", "left", "right"}:
            raise ValueError("shard_side must be 'auto', 'left', or 'right'")

        if not isinstance(num_shards, int):
            raise ValueError("`num_shards` must be provided as an integer.")
        if not 2 <= num_shards <= 50:
            raise ValueError("`num_shards` must be between 2 and 50 (inclusive).")

        input_tables = self._linker._input_tables_dict
        if not input_tables:
            raise ValueError("No input tables registered on the linker.")

        if any(
            col.unquote().name.lower() == "shard"
            for df in input_tables.values()
            for col in df.columns
        ):
            raise ValueError(
                "Input tables already contain a column named 'shard'. Splink "
                "cannot safely overwrite user data when running sharded "
                "predictions."
            )

        tables_to_drop: list[SplinkDataFrame] = []
        shard_predictions: list[SplinkDataFrame] = []
        combined_predictions: SplinkDataFrame | None = None

        aliases = list(input_tables.keys())
        (
            resolved_shard_side,
            target_alias,
            counts_for_logging,
        ) = self._resolve_shard_target(
            shard_side,
            aliases,
            input_tables,
            tables_to_drop,
        )

        shard_input_tables = self._materialise_sharded_inputs(
            input_tables,
            target_alias,
            num_shards,
            tables_to_drop,
        )
        if resolved_shard_side == "left":
            ordered_aliases = [target_alias] + [a for a in aliases if a != target_alias]
        else:
            ordered_aliases = [a for a in aliases if a != target_alias]
            ordered_aliases.append(target_alias)

        reordered_input_tables = {
            alias: shard_input_tables[alias] for alias in ordered_aliases
        }

        self._log_shard_selection(
            run_identifier,
            resolved_shard_side,
            shard_side,
            num_shards,
            counts_for_logging,
        )

        original_blocking_rules = (
            self._linker._settings_obj._blocking_rules_to_generate_predictions
        )
        base_rule_sqls = [br.blocking_rule_sql for br in original_blocking_rules]
        original_input_tables = self._linker._input_tables_dict

        cache = self._linker._intermediate_table_cache
        cache.invalidate_cache()

        try:
            self._linker._input_tables_dict = reordered_input_tables
            cache.invalidate_cache()

            for shard_number in range(1, num_shards + 1):
                cache.invalidate_cache()

                sharded_rules = self._blocking_rules_from_sql_with_shard(
                    base_rule_sqls,
                    shard_number,
                    resolved_shard_side,
                    run_identifier,
                )
                self._linker._settings_obj._blocking_rules_to_generate_predictions = (
                    sharded_rules
                )

                logger.debug(
                    "predict_sharded run %s processing shard %d/%d",
                    run_identifier,
                    shard_number,
                    num_shards,
                )

                prediction = self.predict(
                    threshold_match_probability=threshold_match_probability,
                    threshold_match_weight=threshold_match_weight,
                    materialise_after_computing_term_frequencies=(
                        materialise_after_computing_term_frequencies
                    ),
                    materialise_blocked_pairs=materialise_blocked_pairs,
                )
                shard_predictions.append(prediction)
                self._cleanup_intermediates_after_predict()
                self._drop_blocking_intermediates_if_present()

            combined_predictions = self._combine_shard_predictions(shard_predictions)
            combined_predictions = self._drop_shard_columns_if_present(
                combined_predictions
            )
        finally:
            self._linker._settings_obj._blocking_rules_to_generate_predictions = (
                original_blocking_rules
            )
            self._linker._input_tables_dict = original_input_tables
            cache.invalidate_cache()

            for table in tables_to_drop:
                table.drop_table_from_database_and_remove_from_cache()

            for prediction in shard_predictions:
                prediction.drop_table_from_database_and_remove_from_cache()

        if combined_predictions is None:
            raise SplinkException("Failed to create sharded predictions.")

        combined_predictions.templated_name = "__splink__df_predict"
        return combined_predictions

    def _cleanup_intermediates_after_predict(self) -> None:
        """Drop heavy intermediate tables produced by `predict`."""
        cache = self._linker._intermediate_table_cache
        for name in ("__splink__df_concat_with_tf", "__splink__df_comparison_vectors"):
            if name in cache:
                try:
                    df = cache.get_with_logging(name)
                    df.drop_table_from_database_and_remove_from_cache()
                except Exception:
                    pass

    def _drop_blocking_intermediates_if_present(self) -> None:
        """Remove blocking intermediates that accumulate during sharded inference."""
        cache = self._linker._intermediate_table_cache
        cache_keys_callable = getattr(cache, "keys", None)
        cache_keys = list(cache_keys_callable()) if callable(cache_keys_callable) else []

        for name in cache_keys:
            if any(token in name for token in ("blocked_id_pairs", "raw_blocked_id_pairs")):
                try:
                    cache.get_with_logging(name).drop_table_from_database_and_remove_from_cache()
                except Exception:
                    pass

        candidates = [
            "__splink__blocked_id_pairs",
            "__splink__raw_blocked_id_pairs",
            "__splink__df_predict",
        ]
        for name in candidates:
            try:
                if name in cache:
                    cache.get_with_logging(name).drop_table_from_database_and_remove_from_cache()
            except Exception:
                pass

    def _resolve_shard_target(
        self,
        shard_side: Literal["auto", "left", "right"],
        aliases: list[str],
        input_tables: dict[str, SplinkDataFrame],
        tables_to_drop: list[SplinkDataFrame],
    ) -> tuple[Literal["left", "right"], str, dict[str, int] | None]:
        """Work out which side to shard and return row counts for logging."""
        if len(aliases) == 1:
            alias = aliases[0]
            if shard_side == "right":
                raise ValueError(
                    "Cannot shard the right side when only one input table is present."
                )
            return "left", alias, None

        left_alias = aliases[0]
        right_alias = aliases[1]

        if shard_side == "left":
            return "left", left_alias, None
        if shard_side == "right":
            return "right", right_alias, None

        left_count = self._row_count_for_table(
            left_alias,
            input_tables[left_alias],
            tables_to_drop,
        )
        right_count = self._row_count_for_table(
            right_alias,
            input_tables[right_alias],
            tables_to_drop,
        )
        counts_for_logging: dict[str, int] = {
            left_alias: left_count,
            right_alias: right_count,
        }

        if left_count >= right_count * 1.1:
            return "left", left_alias, counts_for_logging
        if right_count >= left_count * 1.1:
            return "right", right_alias, counts_for_logging
        return "left", left_alias, counts_for_logging

    def _materialise_sharded_inputs(
        self,
        input_tables: dict[str, SplinkDataFrame],
        target_alias: str,
        num_shards: int,
        tables_to_drop: list[SplinkDataFrame],
    ) -> dict[str, SplinkDataFrame]:
        """Attach a deterministic shard column to each input table."""
        shard_expression = self._shard_assignment_expression(num_shards)
        constant_shard_expression = self._constant_shard_expression()

        sharded_tables: dict[str, SplinkDataFrame] = {}
        for alias, df in input_tables.items():
            expr = shard_expression if alias == target_alias else constant_shard_expression
            sql = f"""
                SELECT
                    t.*,
                    {expr} AS shard
                FROM {df.physical_name} AS t
            """
            sharded_df = self._linker._db_api.sql_to_splink_dataframe_checking_cache(
                sql,
                f"__splink__input_with_shard_{alias}",
                use_cache=False,
            )
            sharded_df.templated_name = df.templated_name
            tables_to_drop.append(sharded_df)
            sharded_tables[alias] = sharded_df

        return sharded_tables

    def _log_shard_selection(
        self,
        run_identifier: str,
        resolved_shard_side: Literal["left", "right"],
        requested_shard_side: str,
        num_shards: int,
        counts_for_logging: dict[str, int] | None,
    ) -> None:
        """Emit a concise log message describing the sharding strategy."""
        if counts_for_logging is None:
            logger.info(
                "predict_sharded run %s using shard_side='%s' (requested='%s') "
                "with num_shards=%d",
                run_identifier,
                resolved_shard_side,
                requested_shard_side,
                num_shards,
            )
            return

        counts_msg = ", ".join(
            f"{alias}={count}" for alias, count in counts_for_logging.items()
        )
        logger.info(
            "predict_sharded run %s selecting shard_side='%s' (requested='%s') "
            "with num_shards=%d based on row counts: %s",
            run_identifier,
            resolved_shard_side,
            requested_shard_side,
            num_shards,
            counts_msg,
        )

    def _row_count_for_table(
        self,
        alias: str,
        df: SplinkDataFrame,
        tables_to_drop: list[SplinkDataFrame],
    ) -> int:
        """Fetch the row count for an input table and ensure the temporary table is cleared."""
        sql = f"SELECT COUNT(*) AS row_count FROM {df.physical_name}"
        count_df = self._linker._db_api.sql_to_splink_dataframe_checking_cache(
            sql,
            f"__splink__row_count_{alias}",
            use_cache=False,
        )
        tables_to_drop.append(count_df)
        records = count_df.as_record_dict(limit=1)
        row_count = records[0]["row_count"] if records else 0
        try:
            return int(row_count)
        except (TypeError, ValueError):
            return int(float(row_count))

    def _shard_assignment_expression(self, num_shards: int) -> str:
        """Deterministically assign a shard, guarding against negative modulo results."""
        colinfo = self._linker._settings_obj.column_info_settings
        uid = colinfo.unique_id_input_column.name
        key_expr = f"(CAST({uid} AS VARCHAR))"
        return f"(((hash({key_expr}) % {num_shards}) + {num_shards}) % {num_shards}) + 1"

    def _constant_shard_expression(self) -> str:
        return "1"

    def _combine_shard_predictions(
        self,
        shard_predictions: Iterable[SplinkDataFrame],
    ) -> SplinkDataFrame:
        """Union shard predictions into a single SplinkDataFrame."""
        sql_parts = [f"SELECT * FROM {df.physical_name}" for df in shard_predictions]
        if not sql_parts:
            raise SplinkException("No shard predictions were generated.")

        union_sql = " UNION ALL ".join(sql_parts)
        combined = self._linker._db_api.sql_to_splink_dataframe_checking_cache(
            union_sql,
            "__splink__df_predict",
            use_cache=False,
        )
        return combined

    def _drop_shard_columns_if_present(
        self,
        df: SplinkDataFrame,
    ) -> SplinkDataFrame:
        """Remove any shard columns introduced during sharded inference."""
        shard_names = {
            "shard",
            "l_shard",
            "r_shard",
            "shard_l",
            "shard_r",
            "left_shard",
            "right_shard",
        }
        columns_to_keep = [
            col.name for col in df.columns if col.unquote().name.lower() not in shard_names
        ]

        if len(columns_to_keep) == len(df.columns):
            return df

        if not columns_to_keep:
            raise SplinkException(
                "Sharded predictions contain no columns after removing shard columns."
            )

        pipeline = CTEPipeline([df])
        select_expr = ", ".join(columns_to_keep)
        pipeline.enqueue_sql(
            f"SELECT {select_expr} FROM {df.templated_name}",
            "__splink__df_predict",
        )

        cleaned = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
        df.drop_table_from_database_and_remove_from_cache()
        return cleaned

    def _blocking_rules_from_sql_with_shard(
        self,
        base_rule_sqls: Iterable[str],
        shard_value: int,
        shard_side: Literal["left", "right"],
        run_identifier: str,
    ) -> list[BlockingRule]:
        """Convert base blocking rule SQL into shard-aware BlockingRule objects."""
        prefix = "l" if shard_side == "left" else "r"
        comment = f"/* splink_shard:{run_identifier}:shard={shard_value}:{prefix} */ "
        condition = f"{prefix}.shard = {shard_value}"

        built: list[BlockingRule] = []
        for sql in base_rule_sqls:
            sql_with_shard = f"{comment} {condition} AND ({sql})"
            br = to_blocking_rule_creator(sql_with_shard).get_blocking_rule(
                self._linker._sql_dialect_str
            )
            br.preceding_rules = []
            built.append(br)

        for i, br in enumerate(built):
            br.add_preceding_rules(built[:i])

        return built

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
            sql_infinity_expression=self._linker._infinity_expression,
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
            self._linker._settings_obj,
            sql_infinity_expression=self._linker._infinity_expression,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sql = f"""
        select * from __splink__df_predict
        where match_weight > {match_weight_threshold}
        """

        pipeline.enqueue_sql(sql, "__splink__find_matches_predictions")

        predictions = self._linker._db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )

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

        sqls = predict_from_comparison_vectors_sqls_using_settings(
            linker._settings_obj,
            sql_infinity_expression=linker._infinity_expression,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        if include_found_by_blocking_rules:
            br_col = _select_found_by_blocking_rules(linker._settings_obj)
            sql = f"""
            select *, {br_col}
            from __splink__df_predict
            """

            pipeline.enqueue_sql(sql, "__splink__found_by_blocking_rules")

        predictions = linker._db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )

        linker._settings_obj._retain_matching_columns = retain_matching_columns
        linker._settings_obj._retain_intermediate_calculation_columns = (
            retain_intermediate_calculation_columns
        )

        return predictions
