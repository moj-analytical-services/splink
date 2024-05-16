from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd
import sqlglot

from ..blocking import (
    BlockingRule,
    _sql_gen_where_condition,
    backend_link_type_options,
    block_using_rules_sqls,
    materialise_exploded_id_tables,
    user_input_link_type_options,
)
from ..blocking_rule_creator import BlockingRuleCreator
from ..blocking_rule_creator_utils import to_blocking_rule_creator
from ..charts import ChartReturnType, cumulative_blocking_rule_comparisons_generated
from ..database_api import AcceptableInputTableType, DatabaseAPISubClass
from ..input_column import InputColumn
from ..misc import calculate_cartesian, ensure_is_iterable
from ..pipeline import CTEPipeline
from ..splink_dataframe import SplinkDataFrame
from ..vertically_concatenate import (
    split_df_concat_with_tf_into_two_tables_sqls,
    vertically_concatenate_sql,
)

logger = logging.getLogger(__name__)


def _number_of_comparisons_generated_by_blocking_rule_post_filters_sqls(
    input_data_dict: dict[str, "SplinkDataFrame"],
    blocking_rule: "BlockingRule",
    link_type: backend_link_type_options,
    db_api: DatabaseAPISubClass,
    unique_id_column_name: str,
    source_dataset_column_name: Optional[str],
) -> list[dict[str, str]]:
    input_dataframes = list(input_data_dict.values())

    two_dataset_link_only = link_type == "link_only" and len(input_dataframes) == 2
    if two_dataset_link_only:
        link_type = "two_dataset_link_only"

    source_dataset_input_column, unique_id_input_column = _process_unique_id_columns(
        unique_id_column_name,
        source_dataset_column_name,
        input_data_dict,
        link_type,
        db_api.sql_dialect.name,
    )
    if source_dataset_input_column:
        unique_id_cols = [source_dataset_input_column, unique_id_input_column]
    else:
        unique_id_cols = [unique_id_input_column]

    where_condition = _sql_gen_where_condition(link_type, unique_id_cols)

    # If it's a link_only or link_and_dedupe and no source_dataset_column_name is
    # provided, it will have been set to a default by _process_unique_id_columns
    if source_dataset_input_column is None:
        source_dataset_column_name = None
    else:
        source_dataset_column_name = source_dataset_input_column.name

    sqls = []

    if two_dataset_link_only:
        input_tablename_l = input_dataframes[0].physical_name
        input_tablename_r = input_dataframes[1].physical_name
    else:
        sql = vertically_concatenate_sql(
            input_data_dict,
            salting_required=False,
            source_dataset_column_name=source_dataset_column_name,
        )
        sqls.append({"sql": sql, "output_table_name": "__splink__df_concat"})

        input_tablename_l = "__splink__df_concat"
        input_tablename_r = "__splink__df_concat"

    sql = f"""
    select count(*) as count_of_pairwise_comparisons_generated

    from {input_tablename_l} as l
    inner join {input_tablename_r} as r
    on
    {blocking_rule.blocking_rule_sql}
    {where_condition}
    """
    sqls.append({"sql": sql, "output_table_name": "__splink__comparions_post_filter"})
    return sqls


def _count_comparisons_from_blocking_rule_pre_filter_conditions_sqls(
    input_data_dict: dict[str, "SplinkDataFrame"],
    blocking_rule: "BlockingRule",
    link_type: str,
    db_api: DatabaseAPISubClass,
) -> list[dict[str, str]]:
    input_dataframes = list(input_data_dict.values())

    join_conditions = blocking_rule._equi_join_conditions
    two_dataset_link_only = link_type == "link_only" and len(input_dataframes) == 2

    sqls = []

    if two_dataset_link_only:
        input_tablename_l = input_dataframes[0].physical_name
        input_tablename_r = input_dataframes[1].physical_name
    else:
        sql = vertically_concatenate_sql(
            input_data_dict, salting_required=False, source_dataset_column_name=None
        )
        sqls.append({"sql": sql, "output_table_name": "__splink__df_concat"})

        input_tablename_l = "__splink__df_concat"
        input_tablename_r = "__splink__df_concat"

    l_cols_sel = []
    r_cols_sel = []
    l_cols_gb = []
    r_cols_gb = []
    using = []
    for (
        i,
        (l_key, r_key),
    ) in enumerate(join_conditions):
        l_cols_sel.append(f"{l_key} as key_{i}")
        r_cols_sel.append(f"{r_key} as key_{i}")
        l_cols_gb.append(l_key)
        r_cols_gb.append(r_key)
        using.append(f"key_{i}")

    l_cols_sel_str = ", ".join(l_cols_sel)
    r_cols_sel_str = ", ".join(r_cols_sel)
    l_cols_gb_str = ", ".join(l_cols_gb)
    r_cols_gb_str = ", ".join(r_cols_gb)
    using_str = ", ".join(using)

    if not join_conditions:
        if two_dataset_link_only:
            sql = f"""
            SELECT
                (SELECT COUNT(*) FROM {input_tablename_l})
                *
                (SELECT COUNT(*) FROM {input_tablename_r})
                    AS count_of_pairwise_comparisons_generated
            """
        else:
            sql = """
            select count(*) * count(*) as count_of_pairwise_comparisons_generated
            from __splink__df_concat

            """
        sqls.append(
            {"sql": sql, "output_table_name": "__splink__total_of_block_counts"}
        )
        return sqls

    sql = f"""
    select {l_cols_sel_str}, count(*) as count_l
    from {input_tablename_l}
    group by {l_cols_gb_str}
    """

    sqls.append(
        {"sql": sql, "output_table_name": "__splink__count_comparisons_from_blocking_l"}
    )

    sql = f"""
    select {r_cols_sel_str}, count(*) as count_r
    from {input_tablename_r}
    group by {r_cols_gb_str}
    """

    sqls.append(
        {"sql": sql, "output_table_name": "__splink__count_comparisons_from_blocking_r"}
    )

    sql = f"""
    select *, count_l, count_r, count_l * count_r as block_count
    from __splink__count_comparisons_from_blocking_l
    inner join __splink__count_comparisons_from_blocking_r
    using ({using_str})
    """

    sqls.append({"sql": sql, "output_table_name": "__splink__block_counts"})

    sql = """
    select cast(sum(block_count) as integer) as count_of_pairwise_comparisons_generated
    from __splink__block_counts
    """

    sqls.append({"sql": sql, "output_table_name": "__splink__total_of_block_counts"})

    return sqls


def _row_counts_per_input_table(
    *,
    splink_df_dict: dict[str, "SplinkDataFrame"],
    link_type: backend_link_type_options,
    source_dataset_input_column: Optional[InputColumn],
    db_api: DatabaseAPISubClass,
) -> "SplinkDataFrame":
    pipeline = CTEPipeline()

    if source_dataset_input_column:
        source_dataset_column_name = source_dataset_input_column.name
    else:
        source_dataset_column_name = None

    sql = vertically_concatenate_sql(
        splink_df_dict,
        salting_required=False,
        source_dataset_column_name=source_dataset_column_name,
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    if link_type == "dedupe_only":
        sql = """
        select count(*) as count
        from __splink__df_concat
        """
    else:
        sql = f"""
        select count(*) as count
        from __splink__df_concat
        group by {source_dataset_column_name}
        """
    pipeline.enqueue_sql(sql, "__splink__df_count")
    return db_api.sql_pipeline_to_splink_dataframe(pipeline)


def _process_unique_id_columns(
    unique_id_column_name: str,
    source_dataset_column_name: Optional[str],
    splink_df_dict: dict[str, "SplinkDataFrame"],
    link_type: backend_link_type_options,
    sql_dialect_name: str,
) -> Tuple[Optional[InputColumn], InputColumn]:
    # Various options:
    # In the dedupe_only case we do need a source dataset column. If it is provided,
    # retain it.  (it'll probably be ignored, but does no harm)
    #
    # link_only, link_and_dedupe cases: The user may have provided a single input
    # table, in which case their input table must contain the source dataset column
    #
    # If the user provides n tables, then we can create the source dataset column
    # for them a default name

    if link_type == "dedupe_only":
        if source_dataset_column_name is None:
            return (
                None,
                InputColumn(unique_id_column_name, sql_dialect=sql_dialect_name),
            )
        else:
            return (
                InputColumn(source_dataset_column_name, sql_dialect=sql_dialect_name),
                InputColumn(unique_id_column_name, sql_dialect=sql_dialect_name),
            )

    if link_type in ("link_only", "link_and_dedupe") and len(splink_df_dict) == 1:
        # Get first item in splink_df_dict
        df = next(iter(splink_df_dict.values()))
        cols = df.columns
        if source_dataset_column_name not in [col.unquote().name for col in cols]:
            raise ValueError(
                "You have provided a single input table with link type 'link_only' or "
                "'link_and_dedupe'. You provided a source_dataset_column_name of "
                f"'{source_dataset_column_name}'.\nThis column was not found "
                "in the input data, so Splink does not know how to split your input "
                "data into multiple tables.\n Either provide multiple input datasets, "
                "or create a source dataset column name in your input table"
            )

    if source_dataset_column_name is None:
        return (
            InputColumn("source_dataset", sql_dialect=sql_dialect_name),
            InputColumn(unique_id_column_name, sql_dialect=sql_dialect_name),
        )
    else:
        return (
            InputColumn(source_dataset_column_name, sql_dialect=sql_dialect_name),
            InputColumn(unique_id_column_name, sql_dialect=sql_dialect_name),
        )


def _cumulative_comparisons_to_be_scored_from_blocking_rules(
    *,
    splink_df_dict: dict[str, "SplinkDataFrame"],
    blocking_rules: List[BlockingRule],
    link_type: backend_link_type_options,
    db_api: DatabaseAPISubClass,
    max_rows_limit: int = int(1e9),
    unique_id_column_name: str,
    source_dataset_column_name: Optional[str],
) -> pd.DataFrame:
    source_dataset_input_column, unique_id_input_column = _process_unique_id_columns(
        unique_id_column_name,
        source_dataset_column_name,
        splink_df_dict,
        link_type,
        db_api.sql_dialect.name,
    )

    # Check none of the blocking rules will create a vast/computationally
    # intractable number of comparisons
    for br in blocking_rules:
        # TODO: Deal properly with exlpoding rules
        count = _count_comparisons_generated_from_blocking_rule(
            splink_df_dict=splink_df_dict,
            blocking_rule=br,
            link_type=link_type,
            db_api=db_api,
            max_rows_limit=max_rows_limit,
            compute_post_filter_count=False,
            unique_id_column_name=unique_id_column_name,
            source_dataset_column_name=source_dataset_column_name,
        )
        count_pre_filter = count[
            "number_of_comparisons_generated_pre_filter_conditions"
        ]

        if float(count_pre_filter) > max_rows_limit:
            # TODO: Use a SplinkException?  Want this to give a sensible message
            # when ocoming from estimate_probability_two_random_records_match
            raise ValueError(
                f"Blocking rule {br.blocking_rule_sql} would create {count_pre_filter} "
                "comparisonns.\nThis exceeds the max_rows_limit of "
                f"{max_rows_limit}.\nPlease tighten the "
                "blocking rule or increase the max_rows_limit."
            )

    rc = _row_counts_per_input_table(
        splink_df_dict=splink_df_dict,
        link_type=link_type,
        source_dataset_input_column=source_dataset_input_column,
        db_api=db_api,
    ).as_record_dict()

    cartesian_count = calculate_cartesian(rc, link_type)

    for n, br in enumerate(blocking_rules):
        br.add_preceding_rules(blocking_rules[:n])

    exploding_br_with_id_tables = materialise_exploded_id_tables(
        link_type,
        blocking_rules,
        db_api,
        splink_df_dict,
        source_dataset_input_column=source_dataset_input_column,
        unique_id_input_column=unique_id_input_column,
    )

    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict,
        salting_required=False,
        source_dataset_column_name=source_dataset_column_name,
    )

    pipeline.enqueue_sql(sql, "__splink__df_concat")

    input_columns = [source_dataset_input_column, unique_id_input_column]
    sql_select_expr = ",".join(
        [item for c in input_columns if c is not None for item in c.l_r_names_as_l_r]
    )

    blocking_input_tablename_l = "__splink__df_concat"

    blocking_input_tablename_r = "__splink__df_concat"
    if len(splink_df_dict) == 2 and link_type == "link_only":
        link_type = "two_dataset_link_only"

    if link_type == "two_dataset_link_only" and source_dataset_column_name is not None:
        sqls = split_df_concat_with_tf_into_two_tables_sqls(
            "__splink__df_concat",
            source_dataset_column_name,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        blocking_input_tablename_l = "__splink__df_concat_left"
        blocking_input_tablename_r = "__splink__df_concat_right"

    sqls = block_using_rules_sqls(
        input_tablename_l=blocking_input_tablename_l,
        input_tablename_r=blocking_input_tablename_r,
        blocking_rules=blocking_rules,
        link_type=link_type,
        set_match_probability_to_one=True,
        unique_id_input_column=unique_id_input_column,
        source_dataset_input_column=source_dataset_input_column,
        columns_to_select_sql=sql_select_expr,
    )

    pipeline.enqueue_list_of_sqls(sqls)

    sql = """
        select
        count(*) as row_count,
        match_key
        from __splink__df_blocked
        group by match_key
        order by cast(match_key as int) asc
    """
    pipeline.enqueue_sql(sql, "__splink__df_count_cumulative_blocks")

    result_df = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_pandas_dataframe()

    # The above table won't include rules that have no matches
    all_rules_df = pd.DataFrame(
        {
            "match_key": [str(i) for i in range(len(blocking_rules))],
            "blocking_rule": [br.blocking_rule_sql for br in blocking_rules],
        }
    )
    if len(result_df) > 0:
        complete_df = all_rules_df.merge(result_df, on="match_key", how="left").fillna(
            {"row_count": 0}
        )

        complete_df["cumulative_rows"] = complete_df["row_count"].cumsum().astype(int)
        complete_df["start"] = complete_df["cumulative_rows"] - complete_df["row_count"]
        complete_df["cartesian"] = cartesian_count

        for c in ["row_count", "cumulative_rows", "cartesian", "start"]:
            complete_df[c] = complete_df[c].astype(int)

    else:
        complete_df = all_rules_df.copy()
        complete_df["row_count"] = 0
        complete_df["cumulative_rows"] = 0
        complete_df["cartesian"] = cartesian_count
        complete_df["start"] = 0

    [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]

    col_order = [
        "blocking_rule",
        "row_count",
        "cumulative_rows",
        "cartesian",
        "match_key",
        "start",
    ]

    return complete_df[col_order]


def _count_comparisons_generated_from_blocking_rule(
    *,
    splink_df_dict: dict[str, "SplinkDataFrame"],
    blocking_rule: BlockingRule,
    link_type: backend_link_type_options,
    db_api: DatabaseAPISubClass,
    compute_post_filter_count: bool,
    max_rows_limit: int = int(1e9),
    unique_id_column_name: str,
    source_dataset_column_name: Optional[str],
) -> dict[str, Union[int, str]]:
    # TODO: if it's an exploding blocking rule, make sure we error out
    pipeline = CTEPipeline()
    sqls = _count_comparisons_from_blocking_rule_pre_filter_conditions_sqls(
        splink_df_dict, blocking_rule, link_type, db_api
    )
    pipeline.enqueue_list_of_sqls(sqls)
    pre_filter_total_df = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    pre_filter_total = pre_filter_total_df.as_record_dict()[0][
        "count_of_pairwise_comparisons_generated"
    ]
    pre_filter_total_df.drop_table_from_database_and_remove_from_cache()

    def add_l_r(sql, table_name):
        tree = sqlglot.parse_one(sql, dialect=db_api.sql_dialect.sqlglot_name)
        for node in tree.find_all(sqlglot.expressions.Column):
            node.set("table", table_name)
        return tree.sql(dialect=db_api.sql_dialect.sqlglot_name)

    equi_join_conditions = [
        add_l_r(i, "l") + " = " + add_l_r(j, "r")
        for i, j in blocking_rule._equi_join_conditions
    ]

    equi_join_conditions_joined = " AND ".join(equi_join_conditions)

    filter_conditions = blocking_rule._filter_conditions
    if filter_conditions == "TRUE":
        filter_conditions = ""

    source_dataset_input_column, unique_id_input_column = _process_unique_id_columns(
        unique_id_column_name,
        source_dataset_column_name,
        splink_df_dict,
        link_type,
        db_api.sql_dialect.name,
    )

    if source_dataset_input_column:
        uid_for_where = [source_dataset_input_column, unique_id_input_column]
    else:
        uid_for_where = [unique_id_input_column]

    join_condition_sql = _sql_gen_where_condition(link_type, uid_for_where)

    if not compute_post_filter_count:
        return {
            "number_of_comparisons_generated_pre_filter_conditions": pre_filter_total,
            "number_of_comparisons_to_be_scored_post_filter_conditions": "not computed",
            "filter_conditions_identified": filter_conditions,
            "equi_join_conditions_identified": equi_join_conditions_joined,
            "inner_join_condition_identified": join_condition_sql,
        }

    if pre_filter_total < max_rows_limit:
        pipeline = CTEPipeline()
        sqls = _number_of_comparisons_generated_by_blocking_rule_post_filters_sqls(
            splink_df_dict,
            blocking_rule,
            link_type,
            db_api,
            unique_id_column_name,
            source_dataset_column_name,
        )
        pipeline.enqueue_list_of_sqls(sqls)
        post_filter_total_df = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        post_filter_total = post_filter_total_df.as_record_dict()[0][
            "count_of_pairwise_comparisons_generated"
        ]
        post_filter_total_df.drop_table_from_database_and_remove_from_cache()
    else:
        post_filter_total = "exceeded max_rows_limit, see warning"

        logger.warning(
            "WARNING:\nComputation of number of comparisons post-filter conditions was "
            f"skipped because the number of comparisons generated by your "
            f"blocking rule exceeded max_rows_limit={max_rows_limit:.2e}."
            "\nIt would be likely to be slow to compute.\nIf you still want to go ahead"
            " increase the value of max_rows_limit argument to above "
            f"{pre_filter_total:.3e}.\nRead more about the definitions here:\n"
            "https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html?h=filter+cond#filter-conditions"
        )

    return {
        "number_of_comparisons_generated_pre_filter_conditions": pre_filter_total,
        "number_of_comparisons_to_be_scored_post_filter_conditions": post_filter_total,
        "filter_conditions_identified": filter_conditions,
        "equi_join_conditions_identified": equi_join_conditions_joined,
        "inner_join_condition_identified": join_condition_sql,
    }


def count_comparisons_from_blocking_rule(
    *,
    table_or_tables: Sequence[AcceptableInputTableType],
    blocking_rule: Union[BlockingRuleCreator, str, Dict[str, Any]],
    link_type: user_input_link_type_options,
    db_api: DatabaseAPISubClass,
    unique_id_column_name: str = "unique_id",
    source_dataset_column_name: Optional[str] = None,
    compute_post_filter_count: bool = True,
    max_rows_limit: int = int(1e9),
) -> dict[str, Union[int, str]]:
    # Ensure what's been passed in is a BlockingRuleCreator
    blocking_rule_creator = to_blocking_rule_creator(blocking_rule).get_blocking_rule(
        db_api.sql_dialect.name
    )

    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    return _count_comparisons_generated_from_blocking_rule(
        splink_df_dict=splink_df_dict,
        blocking_rule=blocking_rule_creator,
        link_type=link_type,
        db_api=db_api,
        compute_post_filter_count=compute_post_filter_count,
        max_rows_limit=max_rows_limit,
        unique_id_column_name=unique_id_column_name,
        source_dataset_column_name=source_dataset_column_name,
    )


def cumulative_comparisons_to_be_scored_from_blocking_rules_data(
    *,
    table_or_tables: Sequence[AcceptableInputTableType],
    blocking_rules: Iterable[Union[BlockingRuleCreator, str, Dict[str, Any]]],
    link_type: user_input_link_type_options,
    db_api: DatabaseAPISubClass,
    unique_id_column_name: str = "unique_id",
    max_rows_limit: int = int(1e9),
    source_dataset_column_name: Optional[str] = None,
) -> pd.DataFrame:
    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    # whilst they're named blocking_rules, this is actually a list of
    # BlockingRuleCreators. The followign code turns them into BlockingRule objects
    blocking_rules = ensure_is_iterable(blocking_rules)

    blocking_rules_as_br: List[BlockingRule] = []
    for br in blocking_rules:
        blocking_rules_as_br.append(
            to_blocking_rule_creator(br).get_blocking_rule(db_api.sql_dialect.name)
        )

    return _cumulative_comparisons_to_be_scored_from_blocking_rules(
        splink_df_dict=splink_df_dict,
        blocking_rules=blocking_rules_as_br,
        link_type=link_type,
        db_api=db_api,
        max_rows_limit=max_rows_limit,
        unique_id_column_name=unique_id_column_name,
        source_dataset_column_name=source_dataset_column_name,
    )


def cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    *,
    table_or_tables: Sequence[AcceptableInputTableType],
    blocking_rules: Iterable[Union[BlockingRuleCreator, str, Dict[str, Any]]],
    link_type: user_input_link_type_options,
    db_api: DatabaseAPISubClass,
    unique_id_column_name: str = "unique_id",
    max_rows_limit: int = int(1e9),
    source_dataset_column_name: Optional[str] = None,
) -> ChartReturnType:
    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    # whilst they're named blocking_rules, this is actually a list of
    # BlockingRuleCreators. The followign code turns them into BlockingRule objects
    blocking_rules = ensure_is_iterable(blocking_rules)

    blocking_rules_as_br: List[BlockingRule] = []
    for br in blocking_rules:
        blocking_rules_as_br.append(
            to_blocking_rule_creator(br).get_blocking_rule(db_api.sql_dialect.name)
        )

    pd_df = _cumulative_comparisons_to_be_scored_from_blocking_rules(
        splink_df_dict=splink_df_dict,
        blocking_rules=blocking_rules_as_br,
        link_type=link_type,
        db_api=db_api,
        max_rows_limit=max_rows_limit,
        unique_id_column_name=unique_id_column_name,
        source_dataset_column_name=source_dataset_column_name,
    )

    return cumulative_blocking_rule_comparisons_generated(
        pd_df.to_dict(orient="records")
    )
