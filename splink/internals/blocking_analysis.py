from __future__ import annotations

import logging
import math
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd
import sqlglot

from splink.internals.blocking import (
    BlockingRule,
    _sql_gen_where_condition,
    backend_link_type_options,
    block_using_rules_sqls,
    materialise_exploded_id_tables,
    user_input_link_type_options,
)
from splink.internals.blocking_rule_creator import BlockingRuleCreator
from splink.internals.blocking_rule_creator_utils import to_blocking_rule_creator
from splink.internals.charts import (
    ChartReturnType,
    cumulative_blocking_rule_comparisons_generated,
)
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.input_column import InputColumn
from splink.internals.misc import calculate_cartesian, ensure_is_iterable
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.vertically_concatenate import (
    split_df_concat_with_tf_into_two_tables_sqls,
    vertically_concatenate_sql,
)

logger = logging.getLogger(__name__)


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
            input_data_dict, salting_required=False, source_dataset_input_column=None
        )
        sqls.append({"sql": sql, "output_table_name": "__splink__df_concat"})

        input_tablename_l = "__splink__df_concat"
        input_tablename_r = "__splink__df_concat"

    sql = blocking_rule.create_blocking_input_sql(
        input_tablename=input_tablename_l,
        input_columns=input_dataframes[0].columns,
    )

    sqls.append({"sql": sql, "output_table_name": "__splink__br_input_l"})

    sql = blocking_rule.create_blocking_input_sql(
        input_tablename=input_tablename_r,
        input_columns=input_dataframes[0].columns,
    )

    sqls.append({"sql": sql, "output_table_name": "__splink__br_input_r"})

    input_tablename_l = "__splink__br_input_l"
    input_tablename_r = "__splink__br_input_r"

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
                (SELECT COUNT(*) FROM {input_tablename_l}) as count_l,
                (SELECT COUNT(*) FROM {input_tablename_r}) as count_r,
                (SELECT COUNT(*) FROM {input_tablename_l})
                *
                (SELECT COUNT(*) FROM {input_tablename_r}) as block_count

            """
        else:
            sql = """
            select
                count(*) as count_l,
                count(*) as count_r,
                count(*) * count(*) as block_count
            from __splink__df_concat
            """
        sqls.append({"sql": sql, "output_table_name": "__splink__block_counts"})
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
    select count_l, count_r, count_l * count_r as block_count
    from __splink__count_comparisons_from_blocking_l
    inner join __splink__count_comparisons_from_blocking_r
    using ({using_str})
    """

    sqls.append({"sql": sql, "output_table_name": "__splink__block_counts"})

    return sqls


def _row_counts_per_input_table(
    *,
    splink_df_dict: dict[str, "SplinkDataFrame"],
    link_type: backend_link_type_options,
    source_dataset_input_column: Optional[InputColumn],
    db_api: DatabaseAPISubClass,
) -> "SplinkDataFrame":
    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict,
        salting_required=False,
        source_dataset_input_column=source_dataset_input_column,
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    if link_type == "dedupe_only":
        sql = """
        select count(*) as count
        from __splink__df_concat
        """
    elif source_dataset_input_column is not None:
        sql = f"""
        select count(*) as count
        from __splink__df_concat
        group by {source_dataset_input_column.name}
        """
    else:
        raise ValueError(
            "If you are using link_only or link_and_dedupe, you must provide a "
            "source_dataset_column_name"
        )
    pipeline.enqueue_sql(sql, "__splink__df_count")
    return db_api.sql_pipeline_to_splink_dataframe(pipeline)


def _process_unique_id_columns(
    unique_id_column_name: str,
    source_dataset_column_name: Optional[str],
    splink_df_dict: dict[str, "SplinkDataFrame"],
    link_type: backend_link_type_options,
    sqglot_dialect: str,
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
                InputColumn(unique_id_column_name, sqlglot_dialect_str=sqglot_dialect),
            )
        else:
            return (
                InputColumn(
                    source_dataset_column_name, sqlglot_dialect_str=sqglot_dialect
                ),
                InputColumn(unique_id_column_name, sqlglot_dialect_str=sqglot_dialect),
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
            InputColumn("source_dataset", sqlglot_dialect_str=sqglot_dialect),
            InputColumn(unique_id_column_name, sqlglot_dialect_str=sqglot_dialect),
        )
    else:
        return (
            InputColumn(source_dataset_column_name, sqlglot_dialect_str=sqglot_dialect),
            InputColumn(unique_id_column_name, sqlglot_dialect_str=sqglot_dialect),
        )


def _cumulative_comparisons_to_be_scored_from_blocking_rules(
    *,
    splink_df_dict: dict[str, "SplinkDataFrame"],
    blocking_rules: List[BlockingRule],
    link_type: backend_link_type_options,
    db_api: DatabaseAPISubClass,
    max_rows_limit: int = int(1e9),
    unique_id_input_column: InputColumn,
    source_dataset_input_column: Optional[InputColumn],
    check_max_rows_limit: bool = True,
) -> pd.DataFrame:
    # Check none of the blocking rules will create a vast/computationally
    # intractable number of comparisons
    if check_max_rows_limit:
        for br in blocking_rules:
            # TODO: Deal properly with exlpoding rules
            count = _count_comparisons_generated_from_blocking_rule(
                splink_df_dict=splink_df_dict,
                blocking_rule=br,
                link_type=link_type,
                db_api=db_api,
                max_rows_limit=max_rows_limit,
                compute_post_filter_count=False,
                unique_id_input_column=unique_id_input_column,
                source_dataset_input_column=source_dataset_input_column,
            )
            count_pre_filter = count[
                "number_of_comparisons_generated_pre_filter_conditions"
            ]

            if float(count_pre_filter) > max_rows_limit:
                # TODO: Use a SplinkException?  Want this to give a sensible message
                # when ocoming from estimate_probability_two_random_records_match
                raise ValueError(
                    f"Blocking rule {br.blocking_rule_sql} would create "
                    f"{count_pre_filter} comparisonns.\nThis exceeds the "
                    f"max_rows_limit of {max_rows_limit}.\nPlease tighten the "
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
        source_dataset_input_column=source_dataset_input_column,
    )

    pipeline.enqueue_sql(sql, "__splink__df_concat")

    blocking_input_tablename_l = "__splink__df_concat"

    blocking_input_tablename_r = "__splink__df_concat"
    if len(splink_df_dict) == 2 and link_type == "link_only":
        link_type = "two_dataset_link_only"

    if source_dataset_input_column:
        source_dataset_column_name = source_dataset_input_column.name
    else:
        source_dataset_column_name = None

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
        unique_id_input_column=unique_id_input_column,
        source_dataset_input_column=source_dataset_input_column,
    )

    pipeline.enqueue_list_of_sqls(sqls)

    sql = """
        select
        count(*) as row_count,
        match_key
        from __splink__blocked_id_pairs
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
    unique_id_input_column: InputColumn,
    source_dataset_input_column: Optional[InputColumn],
) -> dict[str, Union[int, str]]:
    pipeline = CTEPipeline()
    sqls = _count_comparisons_from_blocking_rule_pre_filter_conditions_sqls(
        splink_df_dict, blocking_rule, link_type, db_api
    )
    pipeline.enqueue_list_of_sqls(sqls)

    sql = """
    select cast(sum(block_count) as bigint) as count_of_pairwise_comparisons_generated
    from __splink__block_counts
    """

    pipeline.enqueue_sql(sql=sql, output_table_name="__splink__total_of_block_counts")

    pre_filter_total_df = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    pre_filter_total = pre_filter_total_df.as_record_dict()[0][
        "count_of_pairwise_comparisons_generated"
    ]
    pre_filter_total_df.drop_table_from_database_and_remove_from_cache()

    # This is sometimes the sum() over zero rows, with returns as a nan (flaot)
    # or None.  This result implies a count of zero.
    if pre_filter_total is None or (
        isinstance(pre_filter_total, float) and math.isnan(pre_filter_total)
    ):
        pre_filter_total = 0

    def add_l_r(sql, table_name):
        tree = sqlglot.parse_one(sql, dialect=db_api.sql_dialect.sqlglot_dialect)
        for node in tree.find_all(sqlglot.expressions.Column):
            node.set("table", table_name)
        return tree.sql(dialect=db_api.sql_dialect.sqlglot_dialect)

    equi_join_conditions = [
        add_l_r(i, "l") + " = " + add_l_r(j, "r")
        for i, j in blocking_rule._equi_join_conditions
    ]

    equi_join_conditions_joined = " AND ".join(equi_join_conditions)

    filter_conditions = blocking_rule._filter_conditions
    if filter_conditions == "TRUE":
        filter_conditions = ""

    if source_dataset_input_column:
        uid_for_where = [source_dataset_input_column, unique_id_input_column]
    else:
        uid_for_where = [unique_id_input_column]

    link_type_join_condition_sql = _sql_gen_where_condition(link_type, uid_for_where)

    if not compute_post_filter_count:
        return {
            "number_of_comparisons_generated_pre_filter_conditions": pre_filter_total,
            "number_of_comparisons_to_be_scored_post_filter_conditions": "not computed",
            "filter_conditions_identified": filter_conditions,
            "equi_join_conditions_identified": equi_join_conditions_joined,
            "link_type_join_condition": link_type_join_condition_sql,
        }

    if pre_filter_total < max_rows_limit:
        post_filter_total_df = _cumulative_comparisons_to_be_scored_from_blocking_rules(
            splink_df_dict=splink_df_dict,
            blocking_rules=[blocking_rule],
            link_type=link_type,
            db_api=db_api,
            max_rows_limit=max_rows_limit,
            unique_id_input_column=unique_id_input_column,
            source_dataset_input_column=source_dataset_input_column,
            check_max_rows_limit=False,
        )

        post_filter_total = post_filter_total_df.loc[0, "row_count"].item()
    else:
        post_filter_total = "exceeded max_rows_limit, see warning"

        logger.warning(
            "WARNING:\nComputation of number of comparisons post-filter conditions was "
            "skipped because the number of comparisons generated by your "
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
        "link_type_join_condition": link_type_join_condition_sql,
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
    """Analyse a blocking rule to understand the number of comparisons it will generate.

    Read more about the definition of pre and post filter conditions
    [here]("https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html?h=filter+cond#filter-conditions")

    Args:
        table_or_tables (dataframe, str): Input data
        blocking_rule (Union[BlockingRuleCreator, str, Dict[str, Any]]): The blocking
            rule to analyse
        link_type (user_input_link_type_options): The link type - "link_only",
            "dedupe_only" or "link_and_dedupe"
        db_api (DatabaseAPISubClass): Database API
        unique_id_column_name (str, optional):  Defaults to "unique_id".
        source_dataset_column_name (Optional[str], optional):  Defaults to None.
        compute_post_filter_count (bool, optional): Whether to use a slower methodology
            to calculate how many comparisons will be generated post filter conditions.
            Defaults to True.
        max_rows_limit (int, optional): Calculation of post filter counts will only
            proceed if the fast method returns a value below this limit. Defaults
            to int(1e9).

    Returns:
        dict[str, Union[int, str]]: A dictionary containing the results
    """

    # Ensure what's been passed in is a BlockingRuleCreator
    blocking_rule_creator = to_blocking_rule_creator(blocking_rule).get_blocking_rule(
        db_api.sql_dialect.sql_dialect_str
    )

    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    source_dataset_input_column, unique_id_input_column = _process_unique_id_columns(
        unique_id_column_name,
        source_dataset_column_name,
        splink_df_dict,
        link_type,
        db_api.sql_dialect.sql_dialect_str,
    )

    return _count_comparisons_generated_from_blocking_rule(
        splink_df_dict=splink_df_dict,
        blocking_rule=blocking_rule_creator,
        link_type=link_type,
        db_api=db_api,
        compute_post_filter_count=compute_post_filter_count,
        max_rows_limit=max_rows_limit,
        unique_id_input_column=unique_id_input_column,
        source_dataset_input_column=source_dataset_input_column,
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
    """TODO: Add docstring here"""
    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    # whilst they're named blocking_rules, this is actually a list of
    # BlockingRuleCreators. The followign code turns them into BlockingRule objects
    blocking_rules = ensure_is_iterable(blocking_rules)

    blocking_rules_as_br: List[BlockingRule] = []
    for br in blocking_rules:
        blocking_rules_as_br.append(
            to_blocking_rule_creator(br).get_blocking_rule(
                db_api.sql_dialect.sql_dialect_str
            )
        )

    source_dataset_input_column, unique_id_input_column = _process_unique_id_columns(
        unique_id_column_name,
        source_dataset_column_name,
        splink_df_dict,
        link_type,
        db_api.sql_dialect.sql_dialect_str,
    )

    return _cumulative_comparisons_to_be_scored_from_blocking_rules(
        splink_df_dict=splink_df_dict,
        blocking_rules=blocking_rules_as_br,
        link_type=link_type,
        db_api=db_api,
        max_rows_limit=max_rows_limit,
        unique_id_input_column=unique_id_input_column,
        source_dataset_input_column=source_dataset_input_column,
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
    """TODO: Add docstring here"""
    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    # whilst they're named blocking_rules, this is actually a list of
    # BlockingRuleCreators. The followign code turns them into BlockingRule objects
    blocking_rules = ensure_is_iterable(blocking_rules)

    blocking_rules_as_br: List[BlockingRule] = []
    for br in blocking_rules:
        blocking_rules_as_br.append(
            to_blocking_rule_creator(br).get_blocking_rule(
                db_api.sql_dialect.sql_dialect_str
            )
        )

    source_dataset_input_column, unique_id_input_column = _process_unique_id_columns(
        unique_id_column_name,
        source_dataset_column_name,
        splink_df_dict,
        link_type,
        db_api.sql_dialect.sql_dialect_str,
    )

    pd_df = _cumulative_comparisons_to_be_scored_from_blocking_rules(
        splink_df_dict=splink_df_dict,
        blocking_rules=blocking_rules_as_br,
        link_type=link_type,
        db_api=db_api,
        max_rows_limit=max_rows_limit,
        unique_id_input_column=unique_id_input_column,
        source_dataset_input_column=source_dataset_input_column,
    )

    return cumulative_blocking_rule_comparisons_generated(
        pd_df.to_dict(orient="records")
    )


def n_largest_blocks(
    *,
    table_or_tables: Sequence[AcceptableInputTableType],
    blocking_rule: Union[BlockingRuleCreator, str, Dict[str, Any]],
    link_type: user_input_link_type_options,
    db_api: DatabaseAPISubClass,
    n_largest: int = 5,
) -> "SplinkDataFrame":
    """Find the values responsible for creating the largest blocks of records.

    For example, when blocking on first name and surname, the 'John Smith' block
    might be the largest block of records.  In cases where values are highly skewed
    a few values may be resonsible for generating a large proportion of all comparisons.
    This function helps you find the culprit values.

    The analysis is performed pre filter conditions, read more about what this means
    [here]("https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html?h=filter+cond#filter-conditions")

    Args:
        table_or_tables (dataframe, str): Input data
        blocking_rule (Union[BlockingRuleCreator, str, Dict[str, Any]]): The blocking
            rule to analyse
        link_type (user_input_link_type_options): The link type - "link_only",
            "dedupe_only" or "link_and_dedupe"
        db_api (DatabaseAPISubClass): Database API
        n_largest (int, optional): How many rows to return. Defaults to 5.

    Returns:
        SplinkDataFrame: A dataframe containing the n_largest blocks
    """
    blocking_rule_as_br = to_blocking_rule_creator(blocking_rule).get_blocking_rule(
        db_api.sql_dialect.sql_dialect_str
    )

    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    sqls = _count_comparisons_from_blocking_rule_pre_filter_conditions_sqls(
        splink_df_dict, blocking_rule_as_br, link_type, db_api
    )
    pipeline = CTEPipeline()
    pipeline.enqueue_list_of_sqls(sqls)

    join_conditions = blocking_rule_as_br._equi_join_conditions

    keys = ", ".join(f"key_{i}" for i in range(len(join_conditions)))
    sql = f"""
    select {keys}, count_l, count_r,  count_l * count_r as block_count
    from __splink__count_comparisons_from_blocking_l
    inner join __splink__count_comparisons_from_blocking_r
    using ({keys})
    order by count_l * count_r desc
    limit {n_largest}
    """

    pipeline.enqueue_sql(sql=sql, output_table_name="__splink__block_counts")

    return db_api.sql_pipeline_to_splink_dataframe(pipeline)
