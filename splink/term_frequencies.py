from __future__ import annotations

# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/splink/pull/107
import logging
import warnings
from typing import TYPE_CHECKING

from numpy import arange, ceil, floor, log2
from pandas import concat, cut

from .charts import altair_or_json, load_chart_definition
from .input_column import InputColumn

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker

logger = logging.getLogger(__name__)


def colname_to_tf_tablename(input_column: InputColumn):
    input_column = input_column.unquote().name.replace(" ", "_")
    return f"__splink__df_tf_{input_column}"


def term_frequencies_for_single_column_sql(
    input_column: InputColumn, table_name="__splink__df_concat"
):
    col_name = input_column.name

    sql = f"""
    select
    {col_name}, cast(count(*) as float8) / (select
        count({col_name}) as total from {table_name})
            as {input_column.tf_name}
    from {table_name}
    where {col_name} is not null
    group by {col_name}
    """

    return sql


def _join_tf_to_input_df_sql(linker: Linker):
    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns

    select_cols = []

    for col in tf_cols:
        tbl = colname_to_tf_tablename(col)
        if tbl in linker._intermediate_table_cache:
            tbl = linker._intermediate_table_cache[tbl].physical_name
        tf_col = col.tf_name
        select_cols.append(f"{tbl}.{tf_col}")

    select_cols.insert(0, "__splink__df_concat.*")
    select_cols = ", ".join(select_cols)

    templ = "left join {tbl} on __splink__df_concat.{col} = {tbl}.{col}"

    left_joins = []
    for col in tf_cols:
        tbl = colname_to_tf_tablename(col)
        if tbl in linker._intermediate_table_cache:
            tbl = linker._intermediate_table_cache[tbl].physical_name
        sql = templ.format(tbl=tbl, col=col.name)
        left_joins.append(sql)

    # left_joins = [
    #     templ.format(tbl=colname_to_tf_tablename(col), col=col.name)
    #     for col in tf_cols
    # ]
    left_joins = " ".join(left_joins)

    sql = f"""
    select {select_cols}
    from __splink__df_concat
    {left_joins}
    """

    return sql


def term_frequencies_from_concat_with_tf(input_column):
    sql = f"""
        select
        distinct {input_column.name},
        {input_column.tf_name}
        from __splink__df_concat_with_tf
    """

    return sql


def compute_all_term_frequencies_sqls(linker: Linker) -> list[dict]:
    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns

    if not tf_cols:
        return [
            {
                "sql": "select * from __splink__df_concat",
                "output_table_name": "__splink__df_concat_with_tf",
            }
        ]

    sqls = []
    for tf_col in tf_cols:
        tf_table_name = colname_to_tf_tablename(tf_col)

        if tf_table_name not in linker._intermediate_table_cache:
            sql = term_frequencies_for_single_column_sql(tf_col)
            sql = {"sql": sql, "output_table_name": tf_table_name}
            sqls.append(sql)

    sql = _join_tf_to_input_df_sql(linker)
    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_concat_with_tf",
    }
    sqls.append(sql)

    return sqls


def compute_term_frequencies_from_concat_with_tf(linker: "Linker"):
    """If __splink__df_concat_with_tf already exists in your database,
    reverse engineer the underlying tf tables.

    __splink__df_concat_with_tf is a cached table and often output by
    users to disk, for use at a later point in time. As a result, it
    often exists in the database or is easily accessible by the user,
    while the underlying tf tables are not.
    """

    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns
    cache = linker._intermediate_table_cache

    tf_table = []
    for tf_col in tf_cols:
        tf_table_name = colname_to_tf_tablename(tf_col)

        if tf_table_name not in cache:
            sql = term_frequencies_from_concat_with_tf(tf_col)
            sql = {
                "sql": sql,
                "output_table_name": colname_to_tf_tablename(tf_col),
            }
            tf_table.append(sql)
        else:
            tf_table.append(cache[tf_table_name])

    return tf_table


def comparison_level_to_tf_chart_data(cl: dict):
    df = cl["df_tf"]
    df.columns = ["value", "tf"]
    df = df[df.value.notnull()]

    del cl["df_tf"]
    df = df.assign(**cl)

    # TF match weight scaled by tf_adjustment_weight
    df.loc[:, "log2_bf_tf"] = (
        log2(df.loc[:, "u_probability"] / df.loc[:, "tf"])
        * df.loc[:, "tf_adjustment_weight"]
    )

    # Tidy up columns
    # df = df.drop(columns=["tf", "u_probability", "tf_adjustment_weight"])
    df.rename(
        columns={
            "comparison_vector_value": "gamma",
            "tf_adjustment_column": "tf_col",
            "log2_bayes_factor": "log2_bf",
        },
        inplace=True,
    )
    df["log2_bf_final"] = df["log2_bf_tf"] + df["log2_bf"]

    # Add ranks for sorting/selecting
    df = df.sort_values("log2_bf_tf")
    df["most_freq_rank"] = df.groupby("gamma")["log2_bf_tf"].cumcount()
    df["least_freq_rank"] = df.groupby("gamma")["log2_bf_tf"].cumcount(ascending=False)

    cl["df_out"] = df

    return cl


def tf_adjustment_chart(
    linker: Linker, col, n_most_freq, n_least_freq, vals_to_include, as_dict
):
    # Data for chart
    c = linker._settings_obj._get_comparison_by_output_column_name(col)
    c = c._as_detailed_records

    keys_to_retain = [
        "comparison_vector_value",
        "label_for_charts",
        "tf_adjustment_column",
        "tf_adjustment_weight",
        "tf_minimum_u_value",
        "u_probability",
        "log2_bayes_factor",
    ]

    # Select levels with TF adjustments
    c = [
        {k: cl[k] for k in cl.keys() if k in keys_to_retain}
        for cl in c
        if cl["has_tf_adjustments"]
    ]

    # Add data ("df_tf") to each level
    c = [
        dict(
            cl,
            **{
                "df_tf": linker.compute_tf_table(
                    cl["tf_adjustment_column"]
                ).as_pandas_dataframe()
            },
        )
        for cl in c
    ]

    c = [comparison_level_to_tf_chart_data(cl) for cl in c]
    df = concat([cl["df_out"] for cl in c])
    # Filter values
    selected = False if not vals_to_include else df["value"].isin(vals_to_include)
    least_freq = True if not n_least_freq else df["least_freq_rank"] < n_least_freq
    most_freq = True if not n_most_freq else df["most_freq_rank"] < n_most_freq
    mask = selected | least_freq | most_freq

    vals_not_included = [val for val in vals_to_include if val not in df["value"]]
    if vals_not_included:
        warnings.warn(
            f"Values {vals_not_included} from `vals_to_include` were not found in "
            f"the dataset so are not included in the chart.",
            stacklevel=2,
        )

    # Histogram data
    bin_width = 0.5  # Specify the desired bin width

    min_value = floor(df["log2_bf_final"].min())
    max_value = ceil(df["log2_bf_final"].max())
    bin_edges = arange(min_value, max_value + bin_width, bin_width)

    df["bin"] = cut(df["log2_bf_final"], bins=bin_edges)
    binned_df = (
        df.groupby(["gamma", "bin", "log2_bf"])
        .agg({"log2_bf_final": "count", "log2_bf_tf": "mean"})
        .reset_index()
        .rename(columns={"log2_bf_final": "count"})
    )
    binned_df["bin_start"] = binned_df["bin"].apply(lambda x: x.left).astype("float")
    binned_df["bin_end"] = binned_df["bin"].apply(lambda x: x.right).astype("float")
    binned_df["log2_bf_final"] = (binned_df["bin_start"] + binned_df["bin_end"]) / 2
    binned_df["log2_bf_desc"] = (
        binned_df["bin_start"].astype("str") + "-" + binned_df["bin_end"].astype("str")
    )
    binned_df = binned_df.drop(columns="bin")
    binned_df = binned_df[binned_df["count"] > 0]

    df = df.drop(columns="bin")
    df = df[mask]

    chart_path = "tf_adjustment_chart.json"
    chart = load_chart_definition(chart_path)

    # Complete chart schema
    tf_levels = [cl["comparison_vector_value"] for cl in c]
    labels = [
        f'{cl["label_for_charts"]} (TF col: {cl["tf_adjustment_column"]})' for cl in c
    ]

    df = df[df["gamma"].isin(tf_levels)].sort_values("least_freq_rank")

    chart["datasets"]["data"] = df.to_dict("records")
    chart["datasets"]["hist"] = binned_df.to_dict("records")
    chart["config"]["params"][0]["value"] = max(tf_levels)
    chart["config"]["params"][0]["bind"]["options"] = tf_levels
    chart["config"]["params"][0]["bind"]["labels"] = labels

    # filters = [
    #     f"datum.most_freq_rank < {n_most_freq}",
    #     f"datum.least_freq_rank < {n_least_freq}",
    #     " | ".join([f"datum.value == '{v}'" for v in vals_to_include])
    # ]
    # filter_text = " | ".join(filters)
    # chart["hconcat"][0]["layer"][0]["transform"][2]["filter"] = filter_text
    # chart["hconcat"][0]["layer"][2]["transform"][2]["filter"] = filter_text

    # PLACEHOLDER (until we work out adding a dynamic title based on the filtered data)
    chart["hconcat"][0]["layer"][0]["encoding"]["x"]["title"] = "TF column value"
    chart["hconcat"][0]["layer"][-1]["encoding"]["x"]["title"] = "TF column value"
    chart["hconcat"][0]["layer"][0]["encoding"]["tooltip"][0]["title"] = "Value"

    return altair_or_json(chart, as_dict=as_dict)
