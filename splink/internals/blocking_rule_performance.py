from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from splink.internals.linker import Linker
    from splink.internals.splink_dataframe import SplinkDataFrame


class BlockingRulePerformanceRecord(TypedDict):
    match_key: int
    mw_threshold: float
    total_edges: int
    matches: int
    non_matches: int
    blocking_rule: str


class _BlockingRuleCount(TypedDict):
    total_edges: int
    blocking_rule: str


_MATCH_WEIGHT_THRESHOLD_STEP = 0.25
_MAX_MATCH_WEIGHT_THRESHOLD = 10.0


def _query_as_records(
    splink_dataframe: SplinkDataFrame, sql: str
) -> list[dict[str, Any]]:
    result = splink_dataframe.query_sql(sql)
    try:
        return result.as_record_list()
    finally:
        result.drop_table_from_database_and_remove_from_cache()


def _validate_missing_edges(missing_edges: bool | int) -> None:
    if isinstance(missing_edges, bool):
        return

    if not isinstance(missing_edges, int):
        raise TypeError("missing_edges must be a bool or a non-negative integer")
    if missing_edges < 0:
        raise ValueError("missing_edges must be a non-negative integer")


def blocking_rule_performance_data(
    linker: Linker,
    df_predict: SplinkDataFrame,
    missing_edges: bool | int = False,
) -> list[BlockingRulePerformanceRecord]:
    """Build the records used by the blocking-rule performance chart."""
    _validate_missing_edges(missing_edges)
    minimum_match_weight_record = _query_as_records(
        df_predict,
        "SELECT MIN(match_weight) AS minimum_match_weight FROM {this}",
    )[0]
    minimum_match_weight = minimum_match_weight_record["minimum_match_weight"]
    minimum_match_weight = (
        0.0 if minimum_match_weight is None else float(minimum_match_weight)
    )
    minimum_match_weight = round(
        math.ceil(minimum_match_weight / _MATCH_WEIGHT_THRESHOLD_STEP)
        * _MATCH_WEIGHT_THRESHOLD_STEP,
        10,
    )
    maximum_match_weight = max(minimum_match_weight, _MAX_MATCH_WEIGHT_THRESHOLD)
    number_of_steps = int(
        (maximum_match_weight - minimum_match_weight) / _MATCH_WEIGHT_THRESHOLD_STEP
    )
    match_weight_thresholds = tuple(
        round(
            minimum_match_weight + step * _MATCH_WEIGHT_THRESHOLD_STEP,
            10,
        )
        for step in range(number_of_steps + 1)
    )

    comparison_counts = linker.blocking_analysis.count_comparisons_from_blocking_rules(
        record_sample_proportion=1.0
    )

    counts_by_match_key: dict[int, _BlockingRuleCount] = {
        int(record["match_key"]): {
            "total_edges": int(record["marginal_comparison_count"]),
            "blocking_rule": record["blocking_rule"],
        }
        for record in comparison_counts
    }

    match_count_expressions = []
    for index, threshold in enumerate(match_weight_thresholds):
        match_count_expressions.append(
            "SUM(CASE WHEN "
            f"match_weight >= {threshold} THEN 1 ELSE 0 END) AS matches_{index}"
        )

    match_count_sql = ",\n".join(match_count_expressions)
    matches = _query_as_records(
        df_predict,
        f"""
        SELECT
            CAST(match_key AS INTEGER) AS match_key,
            COUNT(*) AS prediction_count,
            {match_count_sql}
        FROM {{this}}
        GROUP BY match_key
        """,
    )
    matches_by_key_and_threshold: dict[tuple[int, float], int] = {}
    prediction_counts: dict[int, int] = {}
    for record in matches:
        match_key = int(record["match_key"])
        prediction_counts[match_key] = int(record["prediction_count"])
        for index, threshold in enumerate(match_weight_thresholds):
            matches_by_key_and_threshold[(match_key, threshold)] = int(
                record[f"matches_{index}"]
            )

    unexpected_match_keys = sorted(
        set(prediction_counts).difference(counts_by_match_key).difference({-1})
    )
    if unexpected_match_keys:
        raise ValueError(
            "df_predict contains match_key values that are not present in the "
            "linker's blocking rules: "
            f"{unexpected_match_keys}"
        )

    if missing_edges is not False:
        missing_edge_count = (
            prediction_counts.get(-1, 0)
            if missing_edges is True
            else int(missing_edges)
        )
        counts_by_match_key[-1] = {
            "total_edges": missing_edge_count,
            "blocking_rule": "Intra-cluster edges missed by all blocking rules",
        }

    chart_records: list[BlockingRulePerformanceRecord] = []
    for match_key in sorted(counts_by_match_key):
        count_record = counts_by_match_key[match_key]
        total_edges = int(count_record["total_edges"])
        blocking_rule = str(count_record["blocking_rule"])
        for threshold in match_weight_thresholds:
            number_of_matches = matches_by_key_and_threshold.get(
                (match_key, threshold), 0
            )
            if number_of_matches > total_edges:
                raise ValueError(
                    f"df_predict contains {number_of_matches} edges at or above "
                    f"match weight {threshold} for match_key {match_key}, but the "
                    f"total edge count is {total_edges}."
                )
            chart_records.append(
                {
                    "match_key": match_key,
                    "mw_threshold": threshold,
                    "total_edges": total_edges,
                    "matches": number_of_matches,
                    "non_matches": total_edges - number_of_matches,
                    "blocking_rule": blocking_rule,
                }
            )

    return chart_records
