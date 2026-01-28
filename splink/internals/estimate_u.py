from __future__ import annotations

import logging
import time
from copy import deepcopy
from functools import partial
from typing import TYPE_CHECKING, List

from splink.internals.blocking import (
    BlockingRule,
    block_using_rules_sqls,
)
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.constants import LEVEL_NOT_OBSERVED_TEXT
from splink.internals.m_u_records_to_parameters import (
    append_u_probability_to_comparison_level_trained_probabilities,
    m_u_records_to_lookup_dict,
)
from splink.internals.misc import ascii_uid
from splink.internals.pipeline import CTEPipeline
from splink.internals.settings import LinkTypeLiteralType, Settings
from splink.internals.vertically_concatenate import (
    enqueue_df_concat,
    split_df_concat_with_tf_into_two_tables_sqls,
)

from .expectation_maximisation import (
    compute_new_parameters_sql,
    compute_proportions_for_new_parameters,
)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    import pandas as pd

    from splink.internals.comparison import Comparison
    from splink.internals.database_api import DatabaseAPISubClass
    from splink.internals.input_column import InputColumn
    from splink.internals.linker import Linker
    from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


class _MUCountsAccumulator:
    def __init__(self, comparison: "Comparison") -> None:
        self._output_column_name = comparison.output_column_name
        self._label_by_cvv: dict[int, str] = {}
        self._counts_by_cvv: dict[int, list[float]] = {
            int(cl.comparison_vector_value): [0.0, 0.0]
            for cl in comparison._comparison_levels_excluding_null
        }

        comparison_levels = list(comparison._comparison_levels_excluding_null)
        for cl in comparison_levels:
            cvv = int(cl.comparison_vector_value)
            label = cl._label_for_charts_no_duplicates(comparison_levels)
            self._label_by_cvv[cvv] = str(label)

    def update_from_chunk_counts(self, chunk_counts: pd.DataFrame) -> None:
        for r in chunk_counts.itertuples(index=False):
            cvv = int(r.comparison_vector_value)
            totals = self._counts_by_cvv.get(cvv)
            if totals is None:
                continue
            totals[0] += float(r.m_count)
            totals[1] += float(r.u_count)

    def min_u_count(self) -> float:
        if not self._counts_by_cvv:
            return 0.0
        return min(totals[1] for totals in self._counts_by_cvv.values())

    def min_u_count_level(self) -> tuple[float, int | None, str | None]:
        """Return (min_u_count, cvv, label) for the current accumulator."""
        if not self._counts_by_cvv:
            return 0.0, None, None
        min_cvv: int | None = None
        min_u: float | None = None
        for cvv in sorted(self._counts_by_cvv):
            u_count = float(self._counts_by_cvv[cvv][1])
            if (min_u is None) or (u_count < min_u):
                min_u = u_count
                min_cvv = cvv
        assert min_u is not None
        label = self._label_by_cvv.get(min_cvv) if min_cvv is not None else None
        return min_u, min_cvv, label

    def all_levels_meet_min_u_count(self, min_count: int) -> bool:
        return self.min_u_count() >= min_count

    def to_dataframe(self) -> pd.DataFrame:
        import pandas as pd

        return pd.DataFrame(
            [
                {
                    "output_column_name": self._output_column_name,
                    "comparison_vector_value": cvv,
                    "m_count": totals[0],
                    "u_count": totals[1],
                }
                for cvv, totals in sorted(self._counts_by_cvv.items())
            ]
        )

    def pretty_table(self) -> str:
        df = self.to_dataframe().drop(columns=["output_column_name"])
        return df.to_string(index=False)


def _accumulate_u_counts_from_chunk_and_check_min_count(
    *,
    db_api: "DatabaseAPISubClass",
    df_sample: "SplinkDataFrame",
    split_sqls: list[dict[str, str]],
    input_tablename_sample_l: str,
    input_tablename_sample_r: str,
    blocking_rules_for_u: list[BlockingRule],
    link_type: LinkTypeLiteralType,
    source_dataset_input_column: "InputColumn | None",
    unique_id_input_column: "InputColumn",
    comparison: "Comparison",
    blocking_cols: list[str],
    cv_cols: list[str],
    rhs_chunk_num: int,
    rhs_num_chunks: int,
    counts_accumulator: _MUCountsAccumulator,
    min_count_per_level: int | None,
    probe_percent_of_max_pairs: float | None = None,
) -> bool:
    if probe_percent_of_max_pairs is not None:
        logger.info(
            f"  Running probe chunk (~{probe_percent_of_max_pairs:.2f}% of max_pairs)"
        )
    else:
        logger.info(f"  Running chunk {rhs_chunk_num}/{rhs_num_chunks}")

    t0 = time.perf_counter()

    pipeline = CTEPipeline(input_dataframes=[df_sample])

    if split_sqls:
        pipeline.enqueue_list_of_sqls(split_sqls)

    blocking_sqls = block_using_rules_sqls(
        input_tablename_l=input_tablename_sample_l,
        input_tablename_r=input_tablename_sample_r,
        blocking_rules=blocking_rules_for_u,
        link_type=link_type,
        source_dataset_input_column=source_dataset_input_column,
        unique_id_input_column=unique_id_input_column,
        right_chunk=(rhs_chunk_num, rhs_num_chunks),
    )

    pipeline.enqueue_list_of_sqls(blocking_sqls)

    cv_sqls = compute_comparison_vector_values_from_id_pairs_sqls(
        blocking_cols,
        cv_cols,
        input_tablename_l=input_tablename_sample_l,
        input_tablename_r=input_tablename_sample_r,
        source_dataset_input_column=source_dataset_input_column,
        unique_id_input_column=unique_id_input_column,
    )

    pipeline.enqueue_list_of_sqls(cv_sqls)

    # Add dummy match_probability column required by compute_new_parameters_sql
    sql = """
    select *, cast(0.0 as float8) as match_probability
    from __splink__df_comparison_vectors
    """
    pipeline.enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters_sql(
        estimate_without_term_frequencies=False,
        comparisons=[comparison],
    )
    pipeline.enqueue_sql(sql, "__splink__m_u_counts")

    df_params = db_api.sql_pipeline_to_splink_dataframe(pipeline)
    try:
        chunk_counts = df_params.as_pandas_dataframe()
    finally:
        df_params.drop_table_from_database_and_remove_from_cache()

    # Drop lambda row: it isn't additive and we don't use it here anyway
    chunk_counts = chunk_counts[
        chunk_counts.output_column_name != "_probability_two_random_records_match"
    ]

    counts_accumulator.update_from_chunk_counts(chunk_counts)

    chunk_elapsed_s = time.perf_counter() - t0

    logger.debug("\n" + counts_accumulator.pretty_table())

    if min_count_per_level is None:
        logger.info(f"  Chunk took {chunk_elapsed_s:.1f} seconds")
        return False

    min_u, min_cvv, min_label = counts_accumulator.min_u_count_level()
    level_desc = (
        f"{min_label} (cvv={min_cvv})" if (min_label is not None) else str(min_cvv)
    )

    if probe_percent_of_max_pairs is not None:
        logger.info(f"  Min u_count: {min_u:,.0f} for comparison level {level_desc}")
    else:
        logger.info(
            f"  Count of {min_u:,.0f} for level {level_desc}. "
            f"Chunk took {chunk_elapsed_s:.1f} seconds."
        )

    if counts_accumulator.all_levels_meet_min_u_count(min_count_per_level):
        logger.info(
            f"  Exiting early since min count of {min_u:,.0f} exceeds "
            f"min_count_per_level = {min_count_per_level}"
        )
        return True

    if probe_percent_of_max_pairs is None:
        logger.info("  Min u_count not hit, continuing.")

    return False


def _rows_needed_for_n_pairs(n_pairs):
    # Number of pairs generated by cartesian product is
    # p(r) = r(r-1)/2, where r is input rows
    # Solve this for r
    # https://www.wolframalpha.com/input?i=Solve%5Bp%3Dr+*+%28r+-+1%29+%2F+2%2C+r%5D
    sample_rows = 0.5 * ((8 * n_pairs + 1) ** 0.5 + 1)
    return sample_rows


def _proportion_sample_size_link_only(
    row_counts_individual_dfs: List[int], max_pairs: float
) -> tuple[float, float]:
    # total valid links is sum of pairwise product of individual row counts
    # i.e. if frame_counts are [a, b, c, d, ...],
    # total_links = a*b + a*c + a*d + ... + b*c + b*d + ... + c*d + ...
    total_links = (
        sum(row_counts_individual_dfs) ** 2
        - sum([count**2 for count in row_counts_individual_dfs])
    ) / 2
    total_nodes = sum(row_counts_individual_dfs)

    # if we scale each frame by a proportion total_links scales with the square
    # i.e. (our target) max_pairs == proportion^2 * total_links
    proportion = (max_pairs / total_links) ** 0.5
    # sample size is for df_concat_with_tf, i.e. proportion of the total nodes
    sample_size = proportion * total_nodes
    return proportion, sample_size


def estimate_u_values(
    linker: Linker,
    max_pairs: float,
    seed: int | None = None,
    min_count_per_level: int | None = 100,
    num_chunks: int = 10,
) -> None:
    logger.info("----- Estimating u probabilities using random sampling -----")
    logger.info(
        "Estimating u with: "
        f"max_pairs = {max_pairs:,.0f}, min_count_per_level = {min_count_per_level}, "
        f"num_chunks = {num_chunks}"
    )
    if num_chunks < 1:
        raise ValueError("num_chunks must be >= 1")
    pipeline = CTEPipeline()

    pipeline = enqueue_df_concat(linker, pipeline)

    original_settings_obj = linker._settings_obj

    training_linker: Linker = deepcopy(linker)

    settings_obj = training_linker._settings_obj
    settings_obj._retain_matching_columns = False
    settings_obj._retain_intermediate_calculation_columns = False

    db_api = training_linker._db_api

    for cc in settings_obj.comparisons:
        for cl in cc.comparison_levels:
            # TODO: ComparisonLevel: manage access
            cl._tf_adjustment_column = None

    if settings_obj._link_type in ["dedupe_only", "link_and_dedupe"]:
        sql = """
        select count(*) as count
        from __splink__df_concat
        """

        pipeline.enqueue_sql(sql, "__splink__df_concat_count")
        count_dataframe = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        result = count_dataframe.as_record_dict()
        count_dataframe.drop_table_from_database_and_remove_from_cache()
        total_nodes = result[0]["count"]
        sample_size = _rows_needed_for_n_pairs(max_pairs)
        proportion = sample_size / total_nodes

    if settings_obj._link_type == "link_only":
        sql = """
        select count(source_dataset) as count
        from __splink__df_concat
        group by source_dataset
        """
        pipeline.enqueue_sql(sql, "__splink__df_concat_count")
        counts_dataframe = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        result = counts_dataframe.as_record_dict()
        counts_dataframe.drop_table_from_database_and_remove_from_cache()
        frame_counts = [res["count"] for res in result]

        proportion, sample_size = _proportion_sample_size_link_only(
            frame_counts, max_pairs
        )

        total_nodes = sum(frame_counts)

    if proportion >= 1.0:
        proportion = 1.0

    if sample_size > total_nodes:
        sample_size = total_nodes

    table_to_sample_from = "__splink__df_concat"
    # if we are provided a seed, we want to order the table before we sample from it
    # this ensures that the resulting table will be consistent across runs
    # (which is what we want when we are supplying a seed)
    # don't bother when we aren't using a seed as it is needless computation
    if seed is not None:
        uid_colname = settings_obj.column_info_settings.unique_id_input_column.name
        table_to_sample_from = (
            f"(select * from {table_to_sample_from} order by {uid_colname})"
        )

    pipeline = CTEPipeline()
    pipeline = enqueue_df_concat(training_linker, pipeline)

    sql = f"""
    select *
    from {table_to_sample_from}
    {training_linker._random_sample_sql(proportion, sample_size, seed)}
    """

    pipeline.enqueue_sql(sql, "__splink__df_concat_sample")
    df_sample = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    blocking_rules_for_u = [
        BlockingRule("1=1", sql_dialect_str=db_api.sql_dialect.sql_dialect_str)
    ]

    input_tablename_sample_l = "__splink__df_concat_sample"
    input_tablename_sample_r = "__splink__df_concat_sample"

    split_sqls: list[dict[str, str]] = []

    if (
        len(linker._input_tables_dict) == 2
        and linker._settings_obj._link_type == "link_only"
    ):
        split_sqls = split_df_concat_with_tf_into_two_tables_sqls(
            "__splink__df_concat",
            linker._settings_obj.column_info_settings.source_dataset_column_name,
            sample_switch=True,
        )
        input_tablename_sample_l = "__splink__df_concat_sample_left"
        input_tablename_sample_r = "__splink__df_concat_sample_right"

    # At this point we've computed our data sample and we're ready to 'block and count'

    # Only chunk on RHS.  Input data is sample and thus always small enough.
    rhs_num_chunks = num_chunks

    uid_columns = settings_obj.column_info_settings.unique_id_input_columns

    common_blocking_cols: list[str] = []
    for uid_column in uid_columns:
        common_blocking_cols.extend(uid_column.l_r_names_as_l_r)

    for i, comparison in enumerate(settings_obj.comparisons):
        logger.info(
            f"\nEstimating u for: {comparison.output_column_name} "
            f"(Comparison {i+1} of {len(settings_obj.comparisons)})"
        )
        original_comparison = original_settings_obj.comparisons[i]

        counts_accumulator = _MUCountsAccumulator(comparison)

        # Blocking needs UIDs + comparison-specific columns
        blocking_cols = (
            common_blocking_cols + comparison._columns_to_select_for_blocking()
        )

        # Comparison vector needs UIDs + comparison output + match_key
        cv_cols = Settings.columns_to_select_for_comparison_vector_values(
            unique_id_input_columns=uid_columns,
            comparisons=[comparison],
            retain_matching_columns=False,
            additional_columns_to_retain=[],
        )

        use_probe = rhs_num_chunks > 1

        # Bind invariant args once to avoid repetition
        run_chunk = partial(
            _accumulate_u_counts_from_chunk_and_check_min_count,
            db_api=db_api,
            df_sample=df_sample,
            split_sqls=split_sqls,
            input_tablename_sample_l=input_tablename_sample_l,
            input_tablename_sample_r=input_tablename_sample_r,
            blocking_rules_for_u=blocking_rules_for_u,
            link_type=linker._settings_obj._link_type,
            source_dataset_input_column=settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings_obj.column_info_settings.unique_id_input_column,
            comparison=comparison,
            blocking_cols=blocking_cols,
            cv_cols=cv_cols,
            min_count_per_level=min_count_per_level,
        )

        min_count_condition_met = False
        if use_probe:
            probe_multiplier = 10
            probe_rhs_num_chunks = rhs_num_chunks * probe_multiplier
            min_count_condition_met = run_chunk(
                rhs_chunk_num=1,
                rhs_num_chunks=probe_rhs_num_chunks,
                counts_accumulator=counts_accumulator,
                probe_percent_of_max_pairs=100.0 / (rhs_num_chunks * probe_multiplier),
            )

        if not min_count_condition_met:
            if use_probe:
                logger.info(
                    "  Probe did not converge; restarting with normal chunking\n"
                )
                counts_accumulator = _MUCountsAccumulator(comparison)

            for rhs_chunk_num in range(1, rhs_num_chunks + 1):
                min_count_condition_met = run_chunk(
                    rhs_chunk_num=rhs_chunk_num,
                    rhs_num_chunks=rhs_num_chunks,
                    counts_accumulator=counts_accumulator,
                )
                if min_count_condition_met and (min_count_per_level is not None):
                    break

        aggregated_counts_df = counts_accumulator.to_dataframe()
        aggregated_counts_sdf = db_api.register(
            aggregated_counts_df, f"__splink__aggregated_m_u_counts_{ascii_uid(8)}"
        )

        # Convert aggregated counts to proportions (u probabilities)
        param_records = compute_proportions_for_new_parameters(aggregated_counts_sdf)
        aggregated_counts_sdf.drop_table_from_database_and_remove_from_cache(
            force_non_splink_table=True
        )

        # Handling of unobserved levels is consistent with splink 4
        # 'LEVEL_NOT_OBSERVED_TEXT' behaviour whilst enabling the 'break early' check
        # - We explicitly include every level (via enumeration) so that convergence
        #   checks can treat missing GROUP BY rows as 0 counts.
        # - But for the final trained u values, a level with u_count == 0 should be
        #   treated as "not observed" (not as u_probability = 0.0).
        u_count_by_cvv = {
            int(row["comparison_vector_value"]): float(row["u_count"])
            for row in aggregated_counts_df.to_dict("records")
        }
        for r in param_records:
            cvv = int(r["comparison_vector_value"])
            if u_count_by_cvv.get(cvv, 0.0) == 0.0:
                r["u_probability"] = LEVEL_NOT_OBSERVED_TEXT

        m_u_records = param_records
        m_u_records_lookup = m_u_records_to_lookup_dict(m_u_records)

        # Apply estimated u values to the original settings object
        for cl in original_comparison._comparison_levels_excluding_null:
            append_u_probability_to_comparison_level_trained_probabilities(
                cl,
                m_u_records_lookup,
                original_comparison.output_column_name,
                "estimate u by random sampling",
            )

    df_sample.drop_table_from_database_and_remove_from_cache()

    logger.info("\nEstimated u probabilities using random sampling")
