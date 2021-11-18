from functools import reduce
from copy import copy, deepcopy
import warnings

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from typeguard import typechecked

from .charts import load_chart_definition, altair_if_installed_else_json
from .settings import complete_settings_dict, Settings
from .vertically_concat import vertically_concatenate_datasets
from .blocking import block_using_rules
from .gammas import add_gammas
from .estimate import _num_target_rows_to_rows_to_sample


def _equal_spaced_buckets(num_buckets, extent):
    buckets = [x for x in range(num_buckets + 1)]
    span = extent[1] - extent[0]
    buckets = [extent[0] + span * (x / num_buckets) for x in buckets]
    return buckets


@typechecked
def _calc_probability_density(
    df_e: DataFrame,
    spark: SparkSession,
    buckets=None,
    score_colname="match_probability",
    symmetric=True,
):

    """perform splink score histogram calculations / internal function

    Compute a histogram using the provided buckets.

        Args:
            df_e (DataFrame): A dataframe of record comparisons containing a
                splink score, e.g. as produced by the expectation step
            spark (SparkSession): SparkSession object
            score_colname: is the score in another column? defaults to match_probability.  also try match_weight
            buckets: accepts either a list of split points or an integer number that is used
                to create equally spaced split points.  It defaults to 100 equally
                spaced split points
            symmetric : if True then the histogram is symmetric

        Returns:
            (list) : list of rows of histogram bins for appropriate splink score variable ready to be plotted.
    """

    if score_colname == "match_probability":
        extent = (0.0, 1.0)
    else:
        weight_max = df_e.agg({score_colname: "max"}).collect()[0][0]
        weight_min = df_e.agg({score_colname: "min"}).collect()[0][0]
        extent = (weight_min, weight_max)
        if symmetric:
            extent_max = max(abs(weight_max), abs(weight_min))
            extent = (-extent_max, extent_max)

    # if buckets a list then use it. if None... then create default. if integer then create equal bins
    if isinstance(buckets, int) and buckets != 0:
        buckets = _equal_spaced_buckets(buckets, extent)
    elif buckets is None:
        buckets = _equal_spaced_buckets(100, extent)

    buckets.sort()

    # ensure bucket splits are in ascending order
    if score_colname == "match_probability":
        if buckets[0] != 0:
            buckets = [0.0] + buckets

        if buckets[-1] != 1.0:
            buckets = buckets + [1.0]

    hist = df_e.select(score_colname).rdd.flatMap(lambda x: x).histogram(buckets)

    # get bucket from and to points
    bin_low = hist[0]
    bin_high = copy(hist[0])
    bin_low.pop()
    bin_high.pop(0)
    counts = hist[1]

    rows = []
    for item in zip(bin_low, bin_high, counts):
        new_row = {
            "splink_score_bin_low": item[0],
            "splink_score_bin_high": item[1],
            "count_rows": item[2],
        }
        rows.append(new_row)

    for r in rows:
        r["binwidth"] = r["splink_score_bin_high"] - r["splink_score_bin_low"]
        r["freqdensity"] = r["count_rows"] / r["binwidth"]

    sumfreqdens = reduce(lambda a, b: a + b["freqdensity"], rows, 0)

    for r in rows:
        r["normalised"] = r["freqdensity"] / sumfreqdens

    return rows


def _create_probability_density_plot(data):
    """plot score histogram

    Args:
        data (list): A list of rows of histogram bins
            as produced by the _calc_probability_density function
    Returns:
        if altair is installed a plot. if altair is not installed
            then it returns the vega lite chart spec as a dictionary
    """

    hist_def_dict = load_chart_definition("score_histogram.json")
    hist_def_dict["data"]["values"] = data

    return altair_if_installed_else_json(hist_def_dict)


def splink_score_histogram(
    df_e: DataFrame,
    spark: SparkSession,
    buckets=None,
    score_colname=None,
    symmetric=True,
):

    """splink score histogram diagnostic plot public API function

    Compute a histogram using the provided buckets and plot the result.

    Args:
        df_e (DataFrame): A dataframe of record comparisons containing a splink score,
            e.g. as produced by the `get_scored_comparisons` function
        spark (SparkSession): SparkSession object
        score_colname : is the score in another column? defaults to None
        buckets : accepts either a list of split points or an integer number that is used to
            create equally spaced split points. It defaults to 100 equally spaced split points from 0.0 to 1.0
        symmetric : if True then the histogram is symmetric
     Returns:
        if altair library is installed this function returns a histogram plot. if altair is not installed
        then it returns the vega lite chart spec as a dictionary
    """

    rows = _calc_probability_density(
        df_e,
        spark=spark,
        buckets=buckets,
        score_colname=score_colname,
        symmetric=symmetric,
    )

    return _create_probability_density_plot(rows)


def comparison_vector_distribution(df_gammas, sort_by_colname=None):

    spark = df_gammas.sql_ctx.sparkSession

    g_cols = [c for c in df_gammas.columns if c.startswith("gamma_")]
    sel_cols = g_cols
    if sort_by_colname:
        sel_cols = g_cols + [sort_by_colname]
    df_gammas = df_gammas.select(sel_cols)

    cols_expr = ", ".join([f'"{c}"' for c in g_cols])
    cols_expr = ", ".join(g_cols)

    df_gammas.createOrReplaceTempView("df_gammas")

    case_tem = "(case when {g} = -1 then 0 when {g} = 0 then -1 else {g} end)"
    sum_gams = " + ".join([case_tem.format(g=c) for c in g_cols])

    sort_col_expr = ""
    if sort_by_colname:
        sort_col_expr = f", avg({sort_by_colname}) as {sort_by_colname}"

    sql = f"""
    select {cols_expr}, concat_ws(',', {cols_expr}) as concat_gam, {sum_gams} as sum_gam, count(*) as count {sort_col_expr}
    from df_gammas
    group by {cols_expr}
    order by {cols_expr}
    """

    gamma_counts = spark.sql(sql)
    return gamma_counts


def comparison_vector_distribution_chart(
    df_gammas: DataFrame, sort_by_colname=None, symlog=True, symlog_constant=40
):

    gamma_counts = comparison_vector_distribution(
        df_gammas, sort_by_colname=sort_by_colname
    )
    gamma_counts = gamma_counts.toPandas()

    hist_def_dict = load_chart_definition("gamma_histogram.json")
    hist_def_dict["data"]["values"] = gamma_counts.to_dict(orient="records")

    if not symlog:
        del hist_def_dict["encoding"]["y"]["scale"]
    else:
        hist_def_dict["encoding"]["y"]["scale"]["constant"] = symlog_constant

    tooltips = [{"field": "count", "type": "quantitative"}]

    if sort_by_colname:
        score_tt = {"field": sort_by_colname, "type": "quantitative"}
    else:
        score_tt = {"field": "sum_gam", "type": "quantitative"}

    tooltips.append(score_tt)
    g_cols = [c for c in df_gammas.columns if c.startswith("gamma_")]
    g_tts = [{"field": c, "type": "nominal"} for c in g_cols]
    tooltips.extend(g_tts)

    hist_def_dict["encoding"]["tooltip"] = tooltips

    if sort_by_colname:
        hist_def_dict["encoding"]["x"]["sort"]["field"] = sort_by_colname

    return altair_if_installed_else_json(hist_def_dict)


def _melted_comparison_vector_distribution(cvd):

    cvd = cvd.toPandas().reset_index()
    cvd = cvd.rename(columns={"index": "comparison_vector_uid"})

    # Pivot cvd2 from wide format to long format on gamma cols
    all_cols = cvd.columns.tolist()
    index_cols = [c for c in all_cols if not c.startswith("gamma_")]

    cvd_melted = cvd.melt(id_vars=index_cols).sort_values("comparison_vector_uid")
    cvd_melted = cvd_melted.rename(columns={"value": "gamma_value"})
    return cvd_melted


def estimate_null_proportions(
    settings: dict,
    df_or_dfs: DataFrame,
    target_rows: int = 1e6,
):

    # Do not modify settings object provided by user either
    settings = deepcopy(settings)

    # For the purpoes of estimating u values, we will not use any blocking
    settings["blocking_rules"] = []

    # Minimise the columns that are needed for the calculation
    settings["retain_matching_columns"] = False
    settings["retain_intermediate_calculation_columns"] = False

    if type(df_or_dfs) == DataFrame:
        dfs = [df_or_dfs]
    else:
        dfs = df_or_dfs

    spark = dfs[0].sql_ctx.sparkSession

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        settings = complete_settings_dict(settings, spark)

    df = vertically_concatenate_datasets(dfs)

    count_rows = df.count()
    if settings["link_type"] in ["dedupe_only", "link_and_dedupe"]:
        sample_size = _num_target_rows_to_rows_to_sample(target_rows)
        proportion = sample_size / count_rows

    if settings["link_type"] == "link_only":
        sample_size = target_rows ** 0.5
        proportion = sample_size / count_rows

    if proportion >= 1.0:
        proportion = 1.0

    df_s = df.sample(False, proportion)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df_comparison = block_using_rules(settings, df_s, spark)

        df_gammas = add_gammas(df_comparison, settings, spark)

    settings_obj = Settings(settings)

    gamma_cols = [cc.gamma_name for cc in settings_obj.comparison_columns_list]

    sql_template = """

    select count(*)/(select count(*) from df_gammas) as count, {gamma_name} as gamma_value, '{gamma_name}' as colname
    from df_gammas
    group by {gamma_name}
    """

    sqls = [sql_template.format(gamma_name=c) for c in gamma_cols]

    unions = " UNION ALL ".join(sqls)

    df_gammas.createOrReplaceTempView("df_gammas")
    sql = f"""

    with unions as ({unions})

    select * from unions where gamma_value = -1


    """
    return spark.sql(sql)