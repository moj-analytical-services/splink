from math import floor


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
    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_hist_raw",
    }
    sqls.append(sql)

    sql = f"""
    select *, splink_score_bin_low + cast({bin_width} as float) as splink_score_bin_high
    from __splink__df_hist_raw
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_hist",
    }
    sqls.append(sql)

    return sqls


def histogram_data(linker, df_predict, num_bins=100):

    sql = """
    select min(match_weight) as min_weight, max(match_weight) as max_weight from
    __splink__df_predict
    """
    linker._enqueue_sql(sql, "__splink__df_min_max")
    df_min_max = linker._execute_sql_pipeline([df_predict]).as_record_dict()

    min_weight = df_min_max[0]["min_weight"]
    max_weight = df_min_max[0]["max_weight"]

    bins, binwidth = _bins(min_weight, max_weight, num_bins)

    sqls = _hist_sql(binwidth)
    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    df_hist = linker._execute_sql_pipeline([df_predict])

    return df_hist
