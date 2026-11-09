from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypedDict

from splink.internals.blocking import (
    BlockingRule,
    _sql_gen_where_condition,
    backend_link_type_options,
    blocking_rule_to_obj,
    combine_unique_id_input_columns,
    materialise_exploded_id_tables,
)
from splink.internals.misc import join_sql_with_union_all
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.splinkdataframe_utils import get_db_api_from_inputs
from splink.internals.unique_id_concat import _composite_unique_id_from_edges_sql
from splink.internals.vertically_concatenate import (
    split_df_concat_with_tf_into_two_tables_sqls,
    vertically_concatenate_sql,
)

if TYPE_CHECKING:
    from splink.internals.linker import Linker


class BlockingRuleImportanceRecord(TypedDict):
    """Summary of one rule's contribution relative to all the other rules.

    ``is_redundant`` means the rule can be removed *individually* without changing
    the candidate-pair set. If more than one rule is redundant, remove one rule and
    recompute the analysis before removing another: two equivalent rules, for
    example, are each covered by the other but cannot both be removed at once.
    """

    blocking_rule_index: int
    blocking_rule: str
    comparison_count: int
    overlapping_comparison_count: int
    marginal_comparison_count: int
    estimated_marginal_match_count: float
    is_redundant: bool


def _rule_hit_column(rule_index: int) -> str:
    return f"br_hit_{rule_index}"


def _fresh_blocking_rules(blocking_rules: list[BlockingRule]) -> list[BlockingRule]:
    # BlockingRule.preceding_rules is mutable and is populated during prediction.
    # Recreate the rules so each one can be evaluated independently of rule order.
    return [blocking_rule_to_obj(rule.as_dict()) for rule in blocking_rules]


def _pairs_from_blocking_rule(
    linker: Linker,
    blocking_rule: BlockingRule,
    rule_index: int,
) -> SplinkDataFrame:
    settings = linker._settings_obj
    db_api = linker._db_api
    input_tables = linker._input_tables_dict
    column_settings = settings.column_info_settings
    source_dataset_column = column_settings.source_dataset_input_column
    unique_id_column = column_settings.unique_id_input_column

    concat_name = f"__splink__df_concat_for_blocking_rule_{rule_index}"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(
        vertically_concatenate_sql(
            input_tables,
            source_dataset_input_column=source_dataset_column,
        ),
        concat_name,
    )

    input_table_l = concat_name
    input_table_r = concat_name
    effective_link_type: backend_link_type_options = settings._link_type
    if len(input_tables) == 2 and settings._link_type == "link_only":
        if source_dataset_column is None:
            raise ValueError(
                "A source dataset column is required for two-table link_only."
            )
        pipeline.enqueue_list_of_sqls(
            split_df_concat_with_tf_into_two_tables_sqls(
                concat_name,
                source_dataset_column.name,
            )
        )
        input_table_l = f"{concat_name}_left"
        input_table_r = f"{concat_name}_right"
        effective_link_type = "two_dataset_link_only"

    exploding_rules = materialise_exploded_id_tables(
        link_type=effective_link_type,
        blocking_rules=[blocking_rule],
        db_api=db_api,
        splink_df_dict=input_tables,
        source_dataset_input_column=source_dataset_column,
        unique_id_input_column=unique_id_column,
    )

    try:
        unique_id_columns = combine_unique_id_input_columns(
            source_dataset_column,
            unique_id_column,
        )
        where_condition = _sql_gen_where_condition(
            effective_link_type,
            unique_id_columns,
            sql_dialect=blocking_rule.sql_dialect,
        )
        pair_sql = blocking_rule.create_blocked_pairs_sql(
            source_dataset_input_column=source_dataset_column,
            unique_id_input_column=unique_id_column,
            input_tablename_l=input_table_l,
            input_tablename_r=input_table_r,
            where_condition=where_condition,
        )
        pipeline.enqueue_sql(
            f"""
            SELECT DISTINCT join_key_l, join_key_r
            FROM ({pair_sql}) AS rule_pairs
            """,
            f"__splink__blocking_rule_pairs_{rule_index}",
        )
        return db_api.sql_pipeline_to_splink_dataframe(pipeline)
    finally:
        for rule in exploding_rules:
            rule.drop_materialised_id_pairs_dataframe()


def _blocking_rule_hits_data(
    linker: Linker,
    blocking_rules: list[BlockingRule],
) -> SplinkDataFrame:
    """Return one row per blocked pair with a 0/1 hit indicator per rule."""
    fresh_rules = _fresh_blocking_rules(blocking_rules)
    pair_tables: list[SplinkDataFrame] = []
    try:
        for rule_index, blocking_rule in enumerate(fresh_rules):
            pair_tables.append(
                _pairs_from_blocking_rule(linker, blocking_rule, rule_index)
            )

        pipeline = CTEPipeline(pair_tables)
        tagged_pair_sqls = [
            f"""
            SELECT
                {rule_index} AS blocking_rule_index,
                join_key_l,
                join_key_r
            FROM {pair_table.templated_name}
            """
            for rule_index, pair_table in enumerate(pair_tables)
        ]
        pipeline.enqueue_sql(
            join_sql_with_union_all(tagged_pair_sqls),
            "__splink__blocking_rule_hits_long",
        )

        hit_expressions = [
            "MAX(CASE WHEN "
            f"blocking_rule_index = {rule_index} THEN 1 ELSE 0 END) "
            f"AS {_rule_hit_column(rule_index)}"
            for rule_index in range(len(fresh_rules))
        ]
        hit_columns_sql = ",\n".join(hit_expressions)
        pipeline.enqueue_sql(
            f"""
            SELECT
                join_key_l,
                join_key_r,
                {hit_columns_sql}
            FROM __splink__blocking_rule_hits_long
            GROUP BY join_key_l, join_key_r
            """,
            "__splink__blocking_rule_hits_wide",
        )

        hit_columns = [
            _rule_hit_column(rule_index)
            for rule_index in range(len(fresh_rules))
        ]
        number_of_rules_hit = " + ".join(hit_columns)
        pipeline.enqueue_sql(
            f"""
            SELECT *, {number_of_rules_hit} AS number_of_rules_hit
            FROM __splink__blocking_rule_hits_wide
            """,
            "__splink__blocking_rule_hits",
        )
        return linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    finally:
        for pair_table in pair_tables:
            pair_table.drop_table_from_database_and_remove_from_cache()


def _query_as_single_record(
    dataframe: SplinkDataFrame,
    sql: str,
) -> dict[str, Any]:
    result = dataframe.query_sql(sql)
    try:
        records = result.as_record_list()
        return records[0]
    finally:
        result.drop_table_from_database_and_remove_from_cache()


def blocking_rule_importance_data(
    linker: Linker,
    df_predict: SplinkDataFrame,
    blocking_rules: list[BlockingRule],
) -> list[BlockingRuleImportanceRecord]:
    """Summarise the order-independent contribution of each blocking rule."""
    get_db_api_from_inputs([*linker._input_tables_dict.values(), df_predict])
    rule_hits = _blocking_rule_hits_data(linker, blocking_rules)

    unique_id_columns = (
        linker._settings_obj.column_info_settings.unique_id_input_columns
    )
    prediction_join_key_l = _composite_unique_id_from_edges_sql(
        unique_id_columns,
        "l",
        "p",
    )
    prediction_join_key_r = _composite_unique_id_from_edges_sql(
        unique_id_columns,
        "r",
        "p",
    )

    aggregate_expressions = []
    for rule_index in range(len(blocking_rules)):
        hit_column = _rule_hit_column(rule_index)
        aggregate_expressions.extend(
            [
                "COALESCE(SUM(CASE WHEN "
                f"{hit_column} = 1 THEN 1 ELSE 0 END), 0) "
                f"AS comparison_count_{rule_index}",
                "COALESCE(SUM(CASE WHEN "
                f"{hit_column} = 1 AND number_of_rules_hit = 1 "
                "THEN 1 ELSE 0 END), 0) "
                f"AS marginal_comparison_count_{rule_index}",
                "COALESCE(SUM(CASE WHEN "
                f"{hit_column} = 1 AND number_of_rules_hit = 1 "
                "THEN match_probability ELSE 0.0 END), 0.0) "
                f"AS estimated_marginal_match_count_{rule_index}",
            ]
        )
    aggregate_sql = ",\n".join(aggregate_expressions)

    try:
        summary = _query_as_single_record(
            df_predict,
            f"""
            WITH predictions_by_pair AS (
                SELECT
                    {prediction_join_key_l} AS join_key_l,
                    {prediction_join_key_r} AS join_key_r,
                    COUNT(*) AS prediction_count,
                    MAX(match_probability) AS match_probability,
                    SUM(
                        CASE WHEN match_probability IS NULL
                            OR match_probability < 0
                            OR match_probability > 1
                        THEN 1 ELSE 0 END
                    ) AS invalid_probability_count
                FROM {{this}} AS p
                GROUP BY {prediction_join_key_l}, {prediction_join_key_r}
            ),
            scored_rule_hits AS (
                SELECT
                    h.*,
                    p.prediction_count,
                    p.match_probability,
                    p.invalid_probability_count
                FROM {rule_hits.physical_name} AS h
                LEFT JOIN predictions_by_pair AS p
                    ON h.join_key_l = p.join_key_l
                    AND h.join_key_r = p.join_key_r
            )
            SELECT
                COALESCE(SUM(
                    CASE WHEN prediction_count IS NULL THEN 1 ELSE 0 END
                ), 0) AS missing_prediction_count,
                COALESCE(SUM(
                    CASE WHEN prediction_count > 1 THEN 1 ELSE 0 END
                ), 0) AS duplicate_prediction_count,
                COALESCE(SUM(invalid_probability_count), 0)
                    AS invalid_probability_count,
                {aggregate_sql}
            FROM scored_rule_hits
            """,
        )
    finally:
        rule_hits.drop_table_from_database_and_remove_from_cache()

    missing_prediction_count = int(summary["missing_prediction_count"])
    if missing_prediction_count:
        raise ValueError(
            "df_predict does not contain all pairs generated by the blocking rules. "
            f"Missing {missing_prediction_count} pairs. Pass the unfiltered output "
            "of linker.inference.predict()."
        )

    duplicate_prediction_count = int(summary["duplicate_prediction_count"])
    if duplicate_prediction_count:
        raise ValueError(
            "df_predict contains duplicate rows for "
            f"{duplicate_prediction_count} blocked pairs."
        )

    invalid_probability_count = int(summary["invalid_probability_count"])
    if invalid_probability_count:
        raise ValueError(
            "df_predict contains "
            f"{invalid_probability_count} invalid match_probability values."
        )

    records: list[BlockingRuleImportanceRecord] = []
    for rule_index, blocking_rule in enumerate(blocking_rules):
        comparison_count = int(summary[f"comparison_count_{rule_index}"])
        marginal_comparison_count = int(
            summary[f"marginal_comparison_count_{rule_index}"]
        )
        records.append(
            {
                "blocking_rule_index": rule_index,
                "blocking_rule": blocking_rule.blocking_rule_sql,
                "comparison_count": comparison_count,
                "overlapping_comparison_count": (
                    comparison_count - marginal_comparison_count
                ),
                "marginal_comparison_count": marginal_comparison_count,
                "estimated_marginal_match_count": float(
                    summary[f"estimated_marginal_match_count_{rule_index}"]
                ),
                "is_redundant": marginal_comparison_count == 0,
            }
        )

    return records
