from __future__ import annotations

import logging
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    Union,
    cast,
)

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
    CumulativeBlockingRuleComparisonsGeneratedChart,
)
from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.duckdb.duckdb_helpers import record_dicts_from_relation
from splink.internals.em_sampling import (
    _PROBE_SAMPLE_MODULUS,
    _probe_actual_fraction,
    _probe_sample_threshold,
)
from splink.internals.input_column import InputColumn
from splink.internals.misc import (
    calculate_cartesian,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.splinkdataframe_utils import (
    get_db_api_from_inputs,
    splink_dataframes_to_dict,
)
from splink.internals.vertically_concatenate import (
    split_df_concat_with_tf_into_two_tables_sqls,
    vertically_concatenate_sql,
)

logger = logging.getLogger(__name__)


def _as_blocking_rule(
    blocking_rule: Union[BlockingRuleCreator, BlockingRule, str, Dict[str, Any]],
    sql_dialect_str: str,
) -> BlockingRule:
    """Convert user input into a ``BlockingRule``.

    Accepts the usual ``BlockingRuleCreator``/str/dict inputs, but also passes
    through objects that are already ``BlockingRule`` instances (for example the
    rules stored on a linker's settings).
    """
    if isinstance(blocking_rule, BlockingRule):
        return blocking_rule
    return to_blocking_rule_creator(blocking_rule).get_blocking_rule(sql_dialect_str)


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
            input_data_dict, source_dataset_input_column=None
        )
        sqls.append({"sql": sql, "output_table_name": "__splink__df_concat"})

        input_tablename_l = "__splink__df_concat"
        input_tablename_r = "__splink__df_concat"

    if blocking_rule.requires_blocking_input_materialisation:
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
        splink_df_dict, source_dataset_input_column=source_dataset_input_column
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

    # From here on, link_type is "link_only" or "link_and_dedupe"
    # Default source_dataset_column_name if not provided
    effective_source_dataset_col = source_dataset_column_name or "source_dataset"

    if len(splink_df_dict) == 1:
        df = next(iter(splink_df_dict.values()))
        cols = df.columns
        sds_input_col = InputColumn(
            effective_source_dataset_col, sqlglot_dialect_str=sqglot_dialect
        )
        if sds_input_col not in cols:
            raise ValueError(
                "You have provided a single input table with link type 'link_only' or "
                "'link_and_dedupe'. You provided a source_dataset_column_name of "
                f"'{effective_source_dataset_col}'.\nThis column was not found "
                "in the input data, so Splink does not know how to split your input "
                "data into multiple tables.\n Either provide multiple input datasets, "
                "or create a source dataset column name in your input table"
            )

    return (
        InputColumn(effective_source_dataset_col, sqlglot_dialect_str=sqglot_dialect),
        InputColumn(unique_id_column_name, sqlglot_dialect_str=sqglot_dialect),
    )


class CumulativeComparisonRecord(TypedDict):
    blocking_rule: str
    equi_join_conditions_identified: str
    filter_conditions_identified: str
    link_type_join_condition: str
    row_count: int
    cumulative_rows: int
    cartesian: int
    match_key: str
    start: int
    probe_proportion: float
    actual_probe_fraction: float


def _describe_blocking_rule(
    blocking_rule: BlockingRule,
    link_type: backend_link_type_options,
    unique_id_input_column: InputColumn,
    source_dataset_input_column: Optional[InputColumn],
    db_api: DatabaseAPISubClass,
) -> dict[str, str]:
    """Produce human-readable descriptions of the equi-join conditions, filter
    conditions and link-type join condition identified for a blocking rule.

    These are descriptive strings only (no computation against the data); they are
    attached to each record so they can be surfaced in chart tooltips.
    """

    def add_l_r(sql: str, table_name: str) -> str:
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

    return {
        "equi_join_conditions_identified": equi_join_conditions_joined,
        "filter_conditions_identified": filter_conditions,
        "link_type_join_condition": link_type_join_condition_sql,
    }


def _cumulative_comparisons_to_be_scored_from_blocking_rules(
    *,
    splink_df_dict: dict[str, "SplinkDataFrame"],
    blocking_rules: List[BlockingRule],
    link_type: backend_link_type_options,
    db_api: DatabaseAPISubClass,
    unique_id_input_column: InputColumn,
    source_dataset_input_column: Optional[InputColumn],
    probe_proportion: float = 1.0,
) -> list[CumulativeComparisonRecord]:
    # We always create the actual blocked pairs and count them (this handles
    # exploding rules and marginal/cumulative counts).  To make this tractable on
    # large data we apply a deterministic hash-based sample to both sides of the
    # join, count the (much smaller) number of sampled pairs, then scale the result
    # back up by 1 / fraction**2.  probe_proportion=1.0 disables sampling (the
    # threshold equals the modulus, so the sample filter is a no-op) and gives exact
    # counts.
    if not 0 < probe_proportion <= 1:
        raise ValueError(
            f"probe_proportion must be in (0, 1]; got {probe_proportion!r}"
        )

    sample_threshold = _probe_sample_threshold(probe_proportion)
    sample_modulus = _PROBE_SAMPLE_MODULUS
    actual_fraction = _probe_actual_fraction(sample_threshold)

    rc = _row_counts_per_input_table(
        splink_df_dict=splink_df_dict,
        link_type=link_type,
        source_dataset_input_column=source_dataset_input_column,
        db_api=db_api,
    ).as_record_dict()

    cartesian_count = calculate_cartesian(rc, link_type)

    # Descriptive strings per rule (computed before add_preceding_rules mutates
    # state; they don't depend on preceding rules).
    rule_descriptions = [
        _describe_blocking_rule(
            br,
            link_type,
            unique_id_input_column,
            source_dataset_input_column,
            db_api,
        )
        for br in blocking_rules
    ]

    for n, br in enumerate(blocking_rules):
        br.add_preceding_rules(blocking_rules[:n])

    exploding_br_with_id_tables = materialise_exploded_id_tables(
        link_type,
        blocking_rules,
        db_api,
        splink_df_dict,
        source_dataset_input_column=source_dataset_input_column,
        unique_id_input_column=unique_id_input_column,
        sample_threshold=sample_threshold,
        sample_modulus=sample_modulus,
    )

    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict, source_dataset_input_column=source_dataset_input_column
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
        sample_threshold=sample_threshold,
        sample_modulus=sample_modulus,
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

    result_df = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_duckdbpyrelation()
    con = db_api.duckdb_con
    con.register("result_df", result_df)

    # The above table won't include rules that have no matches
    all_rules_table_name = "__splink__df_blocking_rule_counts"
    con.execute(
        f"CREATE OR REPLACE TABLE {all_rules_table_name} "
        "(match_key BIGINT, blocking_rule VARCHAR, cartesian BIGINT);"
    )
    con.executemany(
        f"INSERT INTO {all_rules_table_name} VALUES "
        "($match_key, $blocking_rule, $cartesian);",
        [
            {
                "match_key": str(i),
                "blocking_rule": br.blocking_rule_sql,
                "cartesian": cartesian_count,
            }
            for i, br in enumerate(blocking_rules)
        ],
    )

    table_name = "__splink__cumulative_blocking_rule_counts"
    if len(result_df) > 0:
        con.register(table_name, result_df)
        sql = f"""
            WITH simple_counts AS (
                SELECT
                    rules.blocking_rule,
                    coalesce(counts.row_count, 0) as row_count,
                    cast(rules.match_key as int) as match_key,
                    rules.cartesian
                FROM
                    {all_rules_table_name} AS rules
                LEFT JOIN
                    {table_name} AS counts
                ON
                    rules.match_key = counts.match_key
            )
            SELECT
                blocking_rule,
                row_count,
                sum(row_count) over
                    (
                        order by match_key
                        rows between unbounded preceding and current row
                    ) as cumulative_rows,
                cartesian,
                match_key,
                cumulative_rows - row_count as start,
            FROM
                simple_counts
        """

    else:
        # TODO: can we join onto empty arrow table? if so, we don't need separate case
        # simpler sql as we have no data to join onto
        sql = f"""
            select
                blocking_rule,
                0 as row_count,
                0 as cumulative_rows,
                cartesian,
                match_key,
                0 as start,
            from
                {all_rules_table_name}
        """

    [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]
    complete_df = con.sql(sql)
    counts_data = cast(
        list[CumulativeComparisonRecord], record_dicts_from_relation(complete_df)
    )
    # clean up temporary tables
    con.execute(f"DROP VIEW IF EXISTS {table_name}")
    con.execute(f"DROP TABLE {all_rules_table_name}")

    # The row_count values above are counts of *sampled* pairs.  Scale them back up
    # to estimate the true counts, and recompute the cumulative columns.  When
    # probe_proportion=1.0 the scale is 1.0 (exact).  cartesian is exact (it does
    # not depend on sampling) so it is left untouched.
    scale = 1.0 / (actual_fraction**2)
    counts_data = sorted(counts_data, key=lambda r: int(r["match_key"]))
    cumulative = 0
    for record in counts_data:
        estimated = int(round(record["row_count"] * scale))
        record["row_count"] = estimated
        cumulative += estimated
        record["cumulative_rows"] = cumulative
        record["start"] = cumulative - estimated

    for record, description in zip(counts_data, rule_descriptions):
        record["equi_join_conditions_identified"] = description[
            "equi_join_conditions_identified"
        ]
        record["filter_conditions_identified"] = description[
            "filter_conditions_identified"
        ]
        record["link_type_join_condition"] = description["link_type_join_condition"]
        record["probe_proportion"] = probe_proportion
        record["actual_probe_fraction"] = actual_fraction

    return counts_data


def count_comparisons_from_blocking_rules(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    *,
    blocking_rules: Union[
        BlockingRuleCreator,
        BlockingRule,
        str,
        Dict[str, Any],
        Iterable[Union[BlockingRuleCreator, BlockingRule, str, Dict[str, Any]]],
    ],
    link_type: user_input_link_type_options,
    unique_id_column_name: str = "unique_id",
    source_dataset_column_name: Optional[str] = None,
    probe_proportion: float = 1.0,
) -> list[CumulativeComparisonRecord]:
    """Analyse one or more blocking rules to understand how many comparisons they
    will generate.

    The comparisons are counted by actually creating the blocked pairs (post filter
    conditions), so this correctly handles exploding blocking rules and computes the
    marginal (additional) and cumulative number of comparisons generated by each
    rule.

    A single record is returned per blocking rule (so a single rule yields a
    one-element list).  When multiple rules are provided, ``row_count`` is the
    marginal number of comparisons generated by that rule (excluding pairs already
    generated by preceding rules) and ``cumulative_rows`` is the running total.

    For large datasets, set ``probe_proportion`` below 1.0 to sample both sides of
    the blocking join and scale the result back up.  This is much faster at the cost
    of some accuracy.  ``probe_proportion=1.0`` (the default) computes exact counts.

    Args:
        splink_dataframe_or_dataframes (SplinkDataFrame | Sequence[SplinkDataFrame]):
            Input data
        blocking_rules: A single blocking rule or an iterable of blocking rules to
            analyse.
        link_type (user_input_link_type_options): The link type - "link_only",
            "dedupe_only" or "link_and_dedupe"
        unique_id_column_name (str, optional):  Defaults to "unique_id".
        source_dataset_column_name (Optional[str], optional):  Defaults to None.
        probe_proportion (float, optional): The sampling proportion applied to each
            side of the blocking join.  Values below 1.0 estimate the counts from a
            sample (faster, approximate); 1.0 computes exact counts.  Defaults to
            1.0.

    Returns:
        list[CumulativeComparisonRecord]: One record per blocking rule.
    """
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)
    splink_df_dict = splink_dataframes_to_dict(splink_dataframe_or_dataframes)

    # Allow either a single blocking rule or an iterable of them.  A single rule
    # may be a dict, which is itself iterable, so we must detect the single-rule
    # types explicitly rather than relying on iterability.
    if isinstance(blocking_rules, (str, dict, BlockingRuleCreator, BlockingRule)):
        blocking_rules_iterable: Iterable[
            Union[BlockingRuleCreator, BlockingRule, str, Dict[str, Any]]
        ] = [blocking_rules]
    else:
        blocking_rules_iterable = list(blocking_rules)

    blocking_rules_as_br: List[BlockingRule] = []
    for br_input in blocking_rules_iterable:
        blocking_rules_as_br.append(
            _as_blocking_rule(br_input, db_api.sql_dialect.sql_dialect_str)
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
        unique_id_input_column=unique_id_input_column,
        source_dataset_input_column=source_dataset_input_column,
        probe_proportion=probe_proportion,
    )


def chart_comparisons_from_blocking_rules(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    *,
    blocking_rules: Union[
        BlockingRuleCreator,
        BlockingRule,
        str,
        Dict[str, Any],
        Iterable[Union[BlockingRuleCreator, BlockingRule, str, Dict[str, Any]]],
    ],
    link_type: user_input_link_type_options,
    unique_id_column_name: str = "unique_id",
    source_dataset_column_name: Optional[str] = None,
    probe_proportion: float = 1.0,
) -> CumulativeBlockingRuleComparisonsGeneratedChart:
    """Produce a chart of the cumulative number of comparisons generated by one or
    more blocking rules.

    See ``count_comparisons_from_blocking_rules`` for details of the underlying
    computation and the meaning of ``probe_proportion``.

    Args:
        splink_dataframe_or_dataframes (SplinkDataFrame | Sequence[SplinkDataFrame]):
            Input data
        blocking_rules: A single blocking rule or an iterable of blocking rules to
            analyse.
        link_type (user_input_link_type_options): The link type - "link_only",
            "dedupe_only" or "link_and_dedupe"
        unique_id_column_name (str, optional):  Defaults to "unique_id".
        source_dataset_column_name (Optional[str], optional):  Defaults to None.
        probe_proportion (float, optional): Defaults to 1.0.

    Returns:
        CumulativeBlockingRuleComparisonsGeneratedChart: The chart.
    """
    cumulative_comparison_records = count_comparisons_from_blocking_rules(
        splink_dataframe_or_dataframes,
        blocking_rules=blocking_rules,
        link_type=link_type,
        unique_id_column_name=unique_id_column_name,
        source_dataset_column_name=source_dataset_column_name,
        probe_proportion=probe_proportion,
    )

    return CumulativeBlockingRuleComparisonsGeneratedChart(
        cumulative_comparison_records
    )


def n_largest_blocks(
    splink_dataframe_or_dataframes: SplinkDataFrame | Sequence[SplinkDataFrame],
    *,
    blocking_rule: Union[BlockingRuleCreator, BlockingRule, str, Dict[str, Any]],
    link_type: user_input_link_type_options,
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
        splink_dataframe_or_dataframes (SplinkDataFrame | Sequence[SplinkDataFrame]):
            Input data
        blocking_rule (Union[BlockingRuleCreator, str, Dict[str, Any]]): The blocking
            rule to analyse
        link_type (user_input_link_type_options): The link type - "link_only",
            "dedupe_only" or "link_and_dedupe"
        n_largest (int, optional): How many rows to return. Defaults to 5.

    Returns:
        SplinkDataFrame: A dataframe containing the n_largest blocks
    """
    db_api = get_db_api_from_inputs(splink_dataframe_or_dataframes)

    blocking_rule_as_br = _as_blocking_rule(
        blocking_rule, db_api.sql_dialect.sql_dialect_str
    )

    splink_df_dict = splink_dataframes_to_dict(splink_dataframe_or_dataframes)

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
