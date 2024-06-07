from math import floor
from typing import TYPE_CHECKING

from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame

if TYPE_CHECKING:
    from splink.internals.linker import Linker


def _bins(min, max, num_bins):
    bin_widths = [0.01, 0.1, 0.2, 0.25, 0.5, 1, 2, 5]

    rough_binwidth = (max - min) / num_bins

    best_bin = bin_widths[0]
    best_bin_diff = abs(best_bin - rough_binwidth)
    for bin_width in bin_widths:
        diff = abs(bin_width - rough_binwidth)
        if diff < best_bin_diff:
            best_bin = bin_width
            best_bin_diff = diff

    bins = []

    this_bin = floor(min / best_bin) * best_bin

    while this_bin < max + best_bin:
        bins.append(this_bin)
        this_bin += best_bin
    return bins, best_bin


def _hist_sql(bin_width):
    sqls = []

    sql = f"""
    select
        {bin_width} * floor(match_weight / {bin_width}) as splink_score_bin_low,
        {bin_width} as binwidth,
        count(*) as count_rows
    from __splink__df_predict
    group by {bin_width} * floor(match_weight / {bin_width})
    order by {bin_width} * floor(match_weight / {bin_width}) asc
    """
    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_hist_raw",
    }
    sqls.append(sql_info)

    sql = f"""
    select *, splink_score_bin_low + cast({bin_width} as float) as splink_score_bin_high
    from __splink__df_hist_raw
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_hist",
    }
    sqls.append(sql_info)

    return sqls


def histogram_data(
    linker: "Linker", df_predict: SplinkDataFrame, num_bins: int = 100
) -> SplinkDataFrame:
    sql = """
    select min(match_weight) as min_weight, max(match_weight) as max_weight from
    __splink__df_predict
    """
    pipeline = CTEPipeline([df_predict])
    pipeline.enqueue_sql(sql, "__splink__df_min_max")

    df_min_max = linker._db_api.sql_pipeline_to_splink_dataframe(
        pipeline
    ).as_record_dict()

    min_weight = df_min_max[0]["min_weight"]
    max_weight = df_min_max[0]["max_weight"]

    bins, binwidth = _bins(min_weight, max_weight, num_bins)

    pipeline = CTEPipeline([df_predict])
    sqls = _hist_sql(binwidth)
    pipeline.enqueue_list_of_sqls(sqls)

    df_hist = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df_hist
