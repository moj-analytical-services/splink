from functools import reduce
from copy import copy, deepcopy
import warnings
import pandas as pd

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from typeguard import typechecked

from .charts import load_chart_definition, altair_if_installed_else_json
from .settings import Settings


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


def comparison_vector_distribution(df_gammas):
    """Generate a pandas dataframe containing the
    distribution of counts of distinct comparison vectors
    in df_gammas or df_e.

    If df_e is provided, the average match_probability
    and match_weight will be output for each comparison vector.

    Args:
        df_gammas (DataFrame): A dataframe of record comparisons
    """

    spark = df_gammas.sql_ctx.sparkSession

    df_gamma_cols = df_gammas.columns

    g_cols = [c for c in df_gamma_cols if c.startswith("gamma_")]
    sel_cols = g_cols.copy()

    if "match_probability" in df_gamma_cols:
        sel_cols.append("match_probability")
    if "match_weight" in df_gamma_cols:
        sel_cols.append("match_weight")

    df_gammas = df_gammas.select(sel_cols)

    cols_expr = ", ".join([f'"{c}"' for c in g_cols])
    cols_expr = ", ".join(g_cols)

    df_gammas.createOrReplaceTempView("df_gammas")

    case_tem = "(case when {g} = -1 then 0 when {g} = 0 then -1 else {g} end)"
    sum_gams = " + ".join([case_tem.format(g=c) for c in g_cols])

    score_col_expr = []
    if "match_probability" in df_gamma_cols:
        score_col_expr.append(f"avg(match_probability) as match_probability")
    if "match_weight" in df_gamma_cols:
        score_col_expr.append(f"avg(match_weight) as match_weight")
    if score_col_expr:
        score_col_expr = ", " + ", ".join(score_col_expr)
    else:
        score_col_expr = ""

    sql = f"""
    select {cols_expr}, concat_ws(',', {cols_expr}) as gam_concat, {sum_gams} as sum_gam, count(*) as count {score_col_expr}
    from df_gammas
    group by {cols_expr}
    order by {cols_expr}
    """

    gamma_counts = spark.sql(sql)
    gamma_counts = gamma_counts.toPandas()
    gamma_counts["proportion_of_comparisons"] = (
        gamma_counts["count"] / gamma_counts["count"].sum()
    )

    gamma_counts = gamma_counts.sort_values("sum_gam")
    if "match_probability" in gamma_counts.columns:
        gamma_counts = gamma_counts.sort_values("match_probability")

    if "match_weight" in gamma_counts.columns:
        gamma_counts = gamma_counts.sort_values("match_weight")

    gamma_counts["cumulative_comparisons"] = gamma_counts[
        "proportion_of_comparisons"
    ].cumsum()

    return gamma_counts


COLOUR_ENCODING_MATCH_PROB = {
    "type": "quantitative",
    "field": "match_probability",
    "scale": {"range": ["red", "orange", "green"], "domain": [0, 0.5, 1]},
}

COLOUR_ENCODING_MATCH_WEIGHT = {
    "type": "quantitative",
    "field": "match_weight",
    "scale": {
        "range": ["red", "red", "orange", "green", "green"],
        "domain": [-100, -10, 0, 10, 100],
    },
}


def comparison_vector_distribution_chart(cvd_df, symlog=True, symlog_constant=40):
    """Generate an Altair chart from the comparison vector distribution
    produced by splink.diagnostics.comparison_vector_distribution()

    Args:
        cvd_df (pandas.DataFrame): Output of splink.diagnostics.comparison_vector_distribution()
        symlog (bool, optional): If true, y axis is symlog. Defaults to True.
        symlog_constant (int, optional): Symlog constant if symlog set to True. Defaults to 40.
    """

    hist_def_dict = load_chart_definition("gamma_histogram.json")
    hist_def_dict["data"]["values"] = cvd_df.to_dict(orient="records")

    if not symlog:
        del hist_def_dict["encoding"]["y"]["scale"]
    else:
        hist_def_dict["encoding"]["y"]["scale"]["constant"] = symlog_constant

    tooltips = [
        {"field": "gam_concat", "type": "nominal"},
        {"field": "count", "type": "quantitative"},
        {"field": "proportion_of_comparisons", "type": "quantitative", "format": ".2%"},
        {"field": "cumulative_comparisons", "type": "quantitative", "format": ".2%"},
    ]

    cvd_columns = list(cvd_df.columns)
    score_tts = []
    if "match_probability" in cvd_columns:
        score_tts.append(
            {"field": "match_probability", "type": "quantitative", "format": ".2%"}
        )
    if "match_weight" in cvd_columns:
        score_tts.append(
            {"field": "match_weight", "type": "quantitative", "format": ".2f"}
        )
    if len(score_tts) == 0:
        score_tts.append({"field": "sum_gam", "type": "quantitative"})

    tooltips.extend(score_tts)
    g_cols = [c for c in cvd_df.columns if c.startswith("gamma_")]
    g_tts = [{"field": c, "type": "nominal"} for c in g_cols]
    tooltips.extend(g_tts)

    hist_def_dict["encoding"]["tooltip"] = tooltips

    hist_def_dict["encoding"]["x"]["sort"]["field"] = "sum_gam"

    if "match_weight" in cvd_columns:
        hist_def_dict["encoding"]["color"] = COLOUR_ENCODING_MATCH_WEIGHT
        hist_def_dict["encoding"]["x"]["sort"]["field"] = "match_weight"

    if "match_probability" in cvd_columns:
        hist_def_dict["encoding"]["color"] = COLOUR_ENCODING_MATCH_PROB
        hist_def_dict["encoding"]["x"]["sort"]["field"] = "match_probability"

    return altair_if_installed_else_json(hist_def_dict)


def _melted_comparison_vector_distribution(cvd):

    cvd = cvd.reset_index()
    cvd = cvd.rename(columns={"index": "comparison_vector_uid"})

    # Pivot cvd2 from wide format to long format on gamma cols
    all_cols = cvd.columns.tolist()
    index_cols = [c for c in all_cols if not c.startswith("gamma_")]

    cvd_melted = cvd.melt(id_vars=index_cols).sort_values("comparison_vector_uid")
    cvd_melted = cvd_melted.rename(
        columns={"value": "gam_val", "variable": "gam_colname"}
    )
    return cvd_melted


def _m_u_table_with_null_adjustment(null_props, settings, spark):
    settings_obj = Settings(settings)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        settings_obj.complete_settings_dict(spark)
    out_data = []
    for cc in settings_obj.comparison_columns_list:
        gam_name = cc.gamma_name
        null_prop = null_props[gam_name]
        populated_prop = 1 - null_prop
        row = {
            "gam_val": -1,
            "gam_colname": gam_name,
            "m_probability": null_prop,
            "u_probability": null_prop,
        }
        out_data.append(row)
        for row in cc.as_rows():
            m = row["m_probability"]
            u = row["u_probability"]
            gam_val = row["gamma_index"]

            row = {
                "gam_val": gam_val,
                "gam_colname": gam_name,
                "m_probability": m * populated_prop,
                "u_probability": u * populated_prop,
            }
            out_data.append(row)
    prob_lookup = pd.DataFrame(out_data)
    return prob_lookup


def get_theoretical_comparison_vector_distribution(df_gammas, actual_cvd, settings):
    """Generate a pandas dataframe containing the distribution of
    distinct comparison vectors that would be expected
    from the m and u values in the settings object if the assumptions
    of the fellegi sunter model held (i.e. conditional independence
    of comparison vector values given match status).
    Only produces output for the patterns of comparison vectors
    in df_gammas

    If df_e is provided, the average match_probability
    and match_weight will be output for each comparison vector.

    Args:
        df_gammas (DataFrame): A dataframe of record comparisons
    """

    spark = df_gammas.sql_ctx.sparkSession

    settings_obj = Settings(settings)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        settings_obj.complete_settings_dict(spark)

    lam = settings_obj["proportion_of_matches"]

    total_cvs = actual_cvd["count"].sum()
    cvd_melted = _melted_comparison_vector_distribution(actual_cvd)

    # df gammas is needed to estimate the proportion of record comparisons that are null
    null_props = estimate_proportion_of_null_comparisons(df_gammas, settings)
    m_u_lookup = _m_u_table_with_null_adjustment(null_props, settings, spark)

    df_cvd_with_m_u = cvd_melted.merge(
        m_u_lookup,
        left_on=["gam_colname", "gam_val"],
        right_on=["gam_colname", "gam_val"],
    )

    gamma_cols = df_gammas.columns
    actual_cvd_cols = list(actual_cvd.columns)

    index_cols = ["comparison_vector_uid", "gam_concat", "sum_gam"]

    if "match_weight" in gamma_cols and "match_weight" in actual_cvd_cols:
        index_cols.append("match_weight")
    if "match_probability" in gamma_cols and "match_probability" in actual_cvd_cols:
        index_cols.append("match_probability")

    pt1 = df_cvd_with_m_u.pivot_table(
        index=index_cols,
        values=["u_probability", "m_probability"],
        aggfunc=pd.Series.product,
    )
    pt1["proportion_of_comparisons"] = (1 - lam) * pt1["u_probability"] + lam * pt1[
        "m_probability"
    ]

    pt2 = df_cvd_with_m_u.pivot_table(
        index=index_cols, columns="gam_colname", values="gam_val", aggfunc="mean"
    )

    final = pt1.join(pt2).reset_index()
    final["proportion_of_comparisons"] = (
        final["proportion_of_comparisons"] / final["proportion_of_comparisons"].sum()
    )
    final["count"] = final["proportion_of_comparisons"] * total_cvs
    final = final.drop(
        ["comparison_vector_uid", "m_probability", "u_probability"], axis=1
    )

    final = final.sort_values("sum_gam")
    if "match_probability" in final.columns:
        final = final.sort_values("match_probability")

    if "match_weight" in final.columns:
        final = final.sort_values("match_weight")

    final["cumulative_comparisons"] = final["proportion_of_comparisons"].cumsum()

    return final


def estimate_proportion_of_null_comparisons(
    df_gammas: DataFrame,
    settings: dict,
):

    spark = df_gammas.sql_ctx.sparkSession

    settings_obj = Settings(settings)

    gamma_cols = [cc.gamma_name for cc in settings_obj.comparison_columns_list]

    sql_template = """

    select count(*)/(select count(*) from df_gammas) as proportion, {gamma_name} as gam_val, '{gamma_name}' as gam_colname
    from df_gammas
    group by {gamma_name}
    """

    sqls = [sql_template.format(gamma_name=c) for c in gamma_cols]

    unions = " UNION ALL ".join(sqls)

    df_gammas.createOrReplaceTempView("df_gammas")
    sql = f"""

    with unions as ({unions})

    select proportion as null_proportion, gam_colname from unions where gam_val = -1

    """
    null_tab = spark.sql(sql).toPandas()
    null_proportions = null_tab.set_index("gam_colname").to_dict(orient="index")

    result = {k: v["null_proportion"] for k, v in null_proportions.items()}

    for c in gamma_cols:
        if c not in result:
            result[c] = 0.0

    return result


def compare_actual_and_theoretical_cvd(actual_cvd, theoretical_cvd):
    """Compare actual to theoretical distribution

    Args:
        actual_cvd: Output of comparison_vector_distribution()
        theoretical_cvd: Output of  get_theoretical_comparison_vector_distribution()

    Returns:
        Pandas dataframe comparing actual to theoretical distribution
    """
    theoretical_cvd = theoretical_cvd[["gam_concat", "count"]].copy()
    theoretical_cvd = theoretical_cvd.rename(columns={"count": "count_theoretical"})

    actual_cvd = actual_cvd.rename(columns={"count": "count_actual"})

    merged = actual_cvd.merge(
        theoretical_cvd, left_on="gam_concat", right_on="gam_concat"
    )

    merged["count_diff"] = merged["count_actual"] - merged["count_theoretical"]
    return merged


def comparison_vector_comparison_chart(
    cvd_comparison_df, symlog=True, symlog_constant=40
):
    """Generate an Altair chart that compares actual
    to theoretical distribution

    Args:
        cvd_df (pandas.DataFrame): Output of splink.diagnostics.compare_actual_and_theoretical_cvd()
        symlog (bool, optional): If true, y axis is symlog. Defaults to True.
        symlog_constant (int, optional): Symlog constant if symlog set to True. Defaults to 40.
    """

    hist_def_dict = load_chart_definition("gamma_histogram.json")
    hist_def_dict["data"]["values"] = cvd_comparison_df.to_dict(orient="records")
    hist_def_dict["encoding"]["y"]["field"] = "count_diff"

    if not symlog:
        del hist_def_dict["encoding"]["y"]["scale"]
    else:
        hist_def_dict["encoding"]["y"]["scale"]["constant"] = symlog_constant

    tooltips = [
        {"field": "gam_concat", "type": "nominal"},
        {"field": "count_actual", "type": "quantitative"},
        {"field": "count_theoretical", "type": "quantitative"},
        {"field": "count_diff", "type": "quantitative"},
    ]

    cvd_columns = list(cvd_comparison_df.columns)

    score_tts = []
    if "match_probability" in cvd_columns:
        score_tts.append(
            {"field": "match_probability", "type": "quantitative", "format": ".2%"}
        )
    if "match_weight" in cvd_columns:
        score_tts.append(
            {"field": "match_weight", "type": "quantitative", "format": ".2f"}
        )
    if len(score_tts) == 0:
        score_tts.append({"field": "sum_gam", "type": "quantitative"})

    tooltips.extend(score_tts)
    g_cols = [c for c in cvd_comparison_df.columns if c.startswith("gamma_")]
    g_tts = [{"field": c, "type": "nominal"} for c in g_cols]
    tooltips.extend(g_tts)

    hist_def_dict["encoding"]["tooltip"] = tooltips

    hist_def_dict["encoding"]["x"]["sort"]["field"] = "sum_gam"

    if "match_weight" in cvd_columns:
        hist_def_dict["encoding"]["color"] = COLOUR_ENCODING_MATCH_WEIGHT
        hist_def_dict["encoding"]["x"]["sort"]["field"] = "match_weight"
    if "match_probability" in cvd_columns:
        hist_def_dict["encoding"]["color"] = COLOUR_ENCODING_MATCH_PROB
        hist_def_dict["encoding"]["x"]["sort"]["field"] = "match_probability"

    return altair_if_installed_else_json(hist_def_dict)