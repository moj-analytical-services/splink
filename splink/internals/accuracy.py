from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Optional

from splink.internals.block_from_labels import block_from_labels
from splink.internals.blocking import BlockingRule
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_sql,
)
from splink.internals.misc import calculate_cartesian
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import predict_from_comparison_vectors_sqls_using_settings
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.sql_transform import move_l_r_table_prefix_to_column_suffix
from splink.internals.vertically_concatenate import (
    compute_df_concat,
    compute_df_concat_with_tf,
)

if TYPE_CHECKING:
    from splink.internals.linker import Linker
    from splink.internals.settings import Settings


def truth_space_table_from_labels_with_predictions_sqls(
    threshold_actual: float = 0.5,
    match_weight_round_to_nearest: float | None = None,
    total_labels: int = None,
    positives_not_captured_by_blocking_rules_scored_as_zero: bool = True,
) -> list[dict[str, str]]:
    """
    Create truth statistics (TP, TN, FP, FN) at each threshold, suitable for
    plotting e.g. on a ROC chart.

    A naive implementation would iterate through each threshold computing
    the truth statistics.  However, if the number of thresholds is large, this
    is very slow.

    This implementation uses a trick to allow the whole table to be computed 'in one'

    But this makes the code quite a lot harder to understand.  See comments below

    The positives_not_captured_by_blocking_rules_scored_as_zero argument
    controls whether we score all the clerical label pairs, irrespective of
    whether they were found by blocking rules, or whether any clerical pair
    not found by blocking rules is scored as zero.

    This is important because, from the users point of view, they might want to
    distinguish the case of 'model scores correctly but blocking rules are flawed'
    from 'model scores incorrectly'

    """
    # Round to match_weight_round_to_nearest.
    # e.g. if it's 0.25, 1.27 gets rounded to 1.25
    if match_weight_round_to_nearest is not None:
        truth_thres_expr = f"""
            cast({match_weight_round_to_nearest} as float) *
            (round(match_weight/{match_weight_round_to_nearest}))
        """
    else:
        truth_thres_expr = "match_weight"

    sqls = []

    # Classify each record as a clerical positive or negative according to the threshold
    sql = f"""
    select
    *,
    {truth_thres_expr} as truth_threshold,
    case when clerical_match_score >= {threshold_actual} then 1
    else 0
    end
    as clerical_positive,
    case when clerical_match_score >= {threshold_actual} then 0
    else 1
    end
    as clerical_negative
    from __splink__labels_with_predictions
    order by match_weight
    """

    sql_info = {"sql": sql, "output_table_name": "__splink__labels_with_pos_neg"}
    sqls.append(sql_info)

    # Override the truth threshold (splink score) for any records
    # not found by blocking rules

    if positives_not_captured_by_blocking_rules_scored_as_zero:
        truth_threshold = """CASE
                            WHEN found_by_blocking_rules then truth_threshold
                            ELSE cast(-999 as float8)
                            END"""
    else:
        truth_threshold = "truth_threshold"

    # (Originally the case statement was inlined in the following cte, but caused
    # an error with the spark parser that it couldn't find the variable
    # found_by_blocking_rules)
    sql = f"""
    select *, {truth_threshold} as truth_threshold_adj
    from __splink__labels_with_pos_neg
    """
    sql_info = {"sql": sql, "output_table_name": "__splink__labels_with_pos_neg_tt_adj"}
    sqls.append(sql_info)

    # For each distinct truth threshold (splink score)
    # compute the number of pairwise comparisons at that splink score
    # and the the count of positive and negative clerical labels
    # for those pairwise comparisons
    # e.g.For records scored at the truth threshold (splink score threshold) of
    # -2.28757, two of our comparisons are in fact matches
    # This single table can be used to derive the whole truth space table
    sql = """
    select
        truth_threshold_adj as truth_threshold,
        count(*) as num_records_in_row,
        sum(clerical_positive) as clerical_positive,
        sum(clerical_negative) as clerical_negative
    from
    __splink__labels_with_pos_neg_tt_adj
    group by truth_threshold_adj
    order by truth_threshold_adj
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped",
    }
    sqls.append(sql_info)

    # By computing cumulative counts on the above table, we can derive the basis for
    # computing the TP, TN, FP, FN counts at each threshold
    # For example, for a given threshold, if we know:
    # The number of labels where splink gave a score about the threshold
    # The number of clerical positives at the threshold
    # The difference between the two is the number of false positives

    sql = """
    select
    truth_threshold,

    (sum(clerical_positive) over (order by truth_threshold desc))
        as cumulative_clerical_positives_at_or_above_threshold,

    (sum(clerical_negative) over (order by truth_threshold))
        - clerical_negative
        as cumulative_clerical_negatives_below_threshold,

    (select sum(clerical_positive) from __splink__labels_with_pos_neg_grouped)
        as total_clerical_positives,

    (select sum(clerical_negative) from __splink__labels_with_pos_neg_grouped)
        as total_clerical_negatives,

    (select sum(num_records_in_row) from __splink__labels_with_pos_neg_grouped)
        as total_clerical_labels,

    -num_records_in_row + sum(num_records_in_row) over (order by truth_threshold)
        as num_labels_scored_below_threshold,

    sum(num_records_in_row)
        over (order by truth_threshold desc) as num_labels_scored_at_or_above_threshold

    from __splink__labels_with_pos_neg_grouped
    order by  truth_threshold
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped_with_stats",
    }
    sqls.append(sql_info)

    # If total comparisons is defined, we want to modify the above table
    # to account for the fact it's missing a bunch of true negatives

    if total_labels is None:
        sql = """
        select * from __splink__labels_with_pos_neg_grouped_with_stats
        """
    else:
        # This code is only needed in the case of labelling from a column
        # rather than a pairwise table.  It's needed because there are some
        # 'implicit' 'ghost' label - i.e. pairs of records which are not in the
        # labels table, but which are still negatives.  This is because
        # we only create the pairwise comparisons that pass the blocking rules
        # or are clerical matches.
        # All of these 'ghost' records are true negatives
        total_labels_str = f"cast({total_labels} as float8)"

        # When we blocked, some clerical negatives would have been found indirectly
        # for comparisons found by blocking rules, but where the id\label column
        # didn't match
        total_additional_clerical_negatives = f"""({total_labels_str} -
            total_clerical_positives - total_clerical_negatives)"""

        sql = f"""
        select
            truth_threshold,
            cumulative_clerical_positives_at_or_above_threshold,

            cumulative_clerical_negatives_below_threshold
                + {total_additional_clerical_negatives}
                as cumulative_clerical_negatives_below_threshold,

            total_clerical_positives,

            {total_labels_str} - total_clerical_positives
                as total_clerical_negatives,

            {total_labels_str} as total_clerical_labels,

            num_labels_scored_below_threshold + {total_additional_clerical_negatives}
                as num_labels_scored_below_threshold,

            num_labels_scored_at_or_above_threshold
        from
        __splink__labels_with_pos_neg_grouped_with_stats
        """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped_with_stats_adj",
    }
    sqls.append(sql_info)

    sql = """
    select
    truth_threshold,
    total_clerical_labels,
    total_clerical_positives as P,
    total_clerical_negatives as N,

    num_labels_scored_at_or_above_threshold -
        cumulative_clerical_positives_at_or_above_threshold as FP,

    cumulative_clerical_positives_at_or_above_threshold as TP,

    num_labels_scored_below_threshold - cumulative_clerical_negatives_below_threshold
        as FN,

    cumulative_clerical_negatives_below_threshold as TN

    from __splink__labels_with_pos_neg_grouped_with_stats_adj
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped_with_truth_stats",
    }
    sqls.append(sql_info)

    sql = """
    select
        truth_threshold,
        power(2, truth_threshold) / (1 + power(2, truth_threshold))
            as match_probability,
        cast(total_clerical_labels as float8) as total_clerical_labels,
        cast(P as float8) as p,
        cast(N as float8) as n,
        cast(TP as float8) as tp,
        cast(TN as float8) as tn,
        cast(FP as float8) as fp,
        cast(FN as float8) as fn,
        cast(P/total_clerical_labels as float8) as P_rate,
        cast(N as float)/total_clerical_labels as N_rate,
        cast(TP as float8)/P as tp_rate,
        cast(TN as float8)/N as tn_rate,
        cast(FP as float8)/N as fp_rate,
        cast(FN as float8)/P as fn_rate,
        case when TP+FP=0 then 1 else cast(TP as float8)/(TP+FP) end as precision,
        cast(TP as float8)/P as recall,
        cast(TN as float8)/N as specificity,
        case when TN+FN=0 then 1 else cast(TN as float8)/(TN+FN) end as npv,
        cast(TP+TN as float8)/(P+N) as accuracy,
        cast(2.0*TP/(2*TP + FN + FP) as float8) as f1,
        cast(5.0*TP/(5*TP + 4*FN + FP) as float8) as f2,
        cast(1.25*TP/(1.25*TP + 0.25*FN + FP) as float8) as f0_5,
        cast(4.0*TP*TN/((4.0*TP*TN) + ((TP + TN)*(FP + FN))) as float8) as p4,
        case when TN+FN=0 or TP+FP=0 or P=0 or N=0 then 0
            else cast((TP*TN)-(FP*FN) as float8)/sqrt((TP+FP)*P*N*(TN+FN)) end as phi

    from __splink__labels_with_pos_neg_grouped_with_truth_stats
    where truth_threshold >= cast(-998 as float8)
    """

    sql_info = {"sql": sql, "output_table_name": "__splink__truth_space_table"}
    sqls.append(sql_info)
    return sqls


def _select_found_by_blocking_rules(settings_obj: "Settings") -> str:
    brs = settings_obj._blocking_rules_to_generate_predictions

    if brs:
        br_strings = [
            move_l_r_table_prefix_to_column_suffix(
                b.blocking_rule_sql, b.sqlglot_dialect
            )
            for b in brs
        ]
        wrapped_br_strings = [f"(coalesce({b}, false))" for b in br_strings]
        full_br_string = " OR ".join(wrapped_br_strings)
        br_col = f" ({full_br_string}) "
    else:
        br_col = " 1=1 "

    return f"{br_col} as found_by_blocking_rules"


def truth_space_table_from_labels_table(
    linker: Linker,
    labels_tablename: str,
    threshold_actual: float = 0.5,
    match_weight_round_to_nearest: Optional[float] = None,
) -> SplinkDataFrame:
    pipeline = CTEPipeline()

    nodes_with_tf = compute_df_concat_with_tf(linker, pipeline)
    pipeline = CTEPipeline([nodes_with_tf])

    sqls = predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename)
    pipeline.enqueue_list_of_sqls(sqls)

    # c_P and c_N are clerical positive and negative, respectively
    sqls = truth_space_table_from_labels_with_predictions_sqls(
        threshold_actual, match_weight_round_to_nearest
    )
    pipeline.enqueue_list_of_sqls(sqls)

    df_truth_space_table = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df_truth_space_table


def truth_space_table_from_labels_column(
    linker: "Linker",
    label_colname: str,
    threshold_actual: float = 0.5,
    match_weight_round_to_nearest: float = None,
    positives_not_captured_by_blocking_rules_scored_as_zero: bool = True,
) -> SplinkDataFrame:
    # First we need to calculate the number of implicit true negatives
    # That is, any pair of records which have a different ID in the labels
    # column are a negative
    link_type = linker._settings_obj._link_type
    if link_type == "dedupe_only":
        group_by_statement = ""
    else:
        group_by_statement = "group by source_dataset"

    pipeline = CTEPipeline()
    concat = compute_df_concat(linker, pipeline)

    pipeline = CTEPipeline([concat])

    sql = f"""
        select count(*) as count
        from {concat.physical_name}
        {group_by_statement}
    """

    pipeline.enqueue_sql(sql, "__splink__cartesian_product")
    cartesian_count = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    row_count_df = cartesian_count.as_record_dict()
    cartesian_count.drop_table_from_database_and_remove_from_cache()

    total_labels = calculate_cartesian(row_count_df, link_type)

    new_matchkey = len(linker._settings_obj._blocking_rules_to_generate_predictions)

    df_predict = _predict_from_label_column_sql(
        linker,
        label_colname,
    )

    sql = f"""
    select
    case
        when ({label_colname}_l = {label_colname}_r)
        then cast(1.0 as float8) else cast(0.0 as float8)
    end AS clerical_match_score,
    not (cast(match_key as int) = {new_matchkey})
        as found_by_blocking_rules,
    *
    from {df_predict.physical_name}
    """

    pipeline = CTEPipeline()

    pipeline.enqueue_sql(sql, "__splink__labels_with_predictions")

    sqls = truth_space_table_from_labels_with_predictions_sqls(
        threshold_actual,
        match_weight_round_to_nearest=match_weight_round_to_nearest,
        total_labels=total_labels,
        positives_not_captured_by_blocking_rules_scored_as_zero=positives_not_captured_by_blocking_rules_scored_as_zero,  # noqa: E501
    )
    pipeline.enqueue_list_of_sqls(sqls)

    df_truth_space_table = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df_truth_space_table


def predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename):
    sqls = block_from_labels(
        linker, labels_tablename, include_clerical_match_score=True
    )

    sql_info = {
        "sql": compute_comparison_vector_values_sql(
            linker._settings_obj._columns_to_select_for_comparison_vector_values,
            include_clerical_match_score=True,
        ),
        "output_table_name": "__splink__df_comparison_vectors",
    }

    sqls.append(sql_info)

    sqls_2 = predict_from_comparison_vectors_sqls_using_settings(
        linker._settings_obj,
        include_clerical_match_score=True,
        sql_infinity_expression=linker._infinity_expression,
    )

    sqls.extend(sqls_2)
    br_col = _select_found_by_blocking_rules(linker._settings_obj)

    sql = f"""
    select *, {br_col}
    from __splink__df_predict
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_predictions",
    }

    # Clearer name than just df_predict
    sqls.append(sql_info)

    return sqls


def prediction_errors_from_labels_table(
    linker: Linker,
    labels_tablename: str,
    include_false_positives: bool = True,
    include_false_negatives: bool = True,
    threshold_match_probability: float = 0.5,
) -> SplinkDataFrame:
    pipeline = CTEPipeline()
    nodes_with_tf = compute_df_concat_with_tf(linker, pipeline)
    pipeline = CTEPipeline([nodes_with_tf])

    sqls = predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename)

    pipeline.enqueue_list_of_sqls(sqls)

    false_positives = f"""
    (clerical_match_score < {threshold_match_probability} and
    match_probability > {threshold_match_probability})
    """

    false_negatives = f"""
    (clerical_match_score > {threshold_match_probability} and
    match_probability < {threshold_match_probability})
    """

    where_conditions = []
    if include_false_positives:
        where_conditions.append(false_positives)

    if include_false_negatives:
        where_conditions.append(false_negatives)

    where_condition = " OR ".join(where_conditions)

    sql = f"""
    select *,
    case
    when {false_positives} then 'FP'
    when {false_negatives} then 'FN'
    END as truth_status

    from __splink__labels_with_predictions
    where
    {where_condition}
    """

    pipeline.enqueue_sql(sql, "__splink__labels_with_fp_fn_status")

    return linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)


def _predict_from_label_column_sql(linker, label_colname):
    # In the case of labels, we use them to block
    # In the case we have a label column, we want to apply the model's blocking rules
    # but add in blocking on the label colname
    linker = deepcopy(linker)
    settings = linker._settings_obj
    brs = settings._blocking_rules_to_generate_predictions

    label_blocking_rule = BlockingRule(
        f"l.{label_colname} = r.{label_colname}",
        sql_dialect_str=linker._sql_dialect_str,
    )
    label_blocking_rule.preceding_rules = brs.copy()
    brs.append(label_blocking_rule)

    # Need the label colname to be in additional columns to retain

    add_cols = settings._additional_column_names_to_retain

    if label_colname not in add_cols:
        settings._additional_column_names_to_retain.append(label_colname)

    # Now we want to create predictions
    df_predict = linker.inference.predict()

    return df_predict


def prediction_errors_from_label_column(
    linker: Linker,
    label_colname: str,
    include_false_positives: bool = True,
    include_false_negatives: bool = True,
    threshold: float = 0.5,
) -> SplinkDataFrame:
    df_predict = _predict_from_label_column_sql(
        linker,
        label_colname,
    )

    # Clerical match score is 1 where the label_colname is equal else zero

    # _predict_from_label_column_sql will add a match key for matching on labels
    new_matchkey = len(linker._settings_obj._blocking_rules_to_generate_predictions)
    pipeline = CTEPipeline()
    sql = f"""
    select
    case
        when ({label_colname}_l = {label_colname}_r)
        then cast(1.0 as float8) else cast(0.0 as float8)
    end AS clerical_match_score,
    not (cast(match_key as int) = {new_matchkey})
        as found_by_blocking_rules,
    *
    from {df_predict.physical_name}
    """

    # found_by_blocking_rules

    pipeline.enqueue_sql(sql, "__splink__predictions_from_label_column")

    false_positives = f"""
    (clerical_match_score < {threshold} and
    match_probability > {threshold})
    """

    false_negatives = f"""
    ((clerical_match_score > {threshold} and
    match_probability < {threshold})
    or
    (clerical_match_score > {threshold} and
     found_by_blocking_rules = False)
    )
    """

    where_conditions = []
    if include_false_positives:
        where_conditions.append(false_positives)

    if include_false_negatives:
        where_conditions.append(false_negatives)

    where_condition = " OR ".join(where_conditions)

    sql = f"""
    select * from __splink__predictions_from_label_column
    where {where_condition}
    """

    pipeline.enqueue_sql(sql, "__splink__predictions_from_label_column_fp_fn_only")

    predictions = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return predictions
