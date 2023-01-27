from benchmarking_functions import (
    append_output_to_timeseries,
    chart_to_markdown,
    format_timeseries_data_table,
    get_chart,
    get_markdown_tables,
    load_timeseries,
)

new_data = append_output_to_timeseries("benchmarking")
this_cpu = new_data["machine_info"]["cpu"]["brand_raw"]

timeseries_df = load_timeseries("benchmarking")


timeseries_df = format_timeseries_data_table(timeseries_df)


markdown = get_markdown_tables(timeseries_df, this_cpu)

chart = get_chart(timeseries_df)

chart_markdown = chart_to_markdown(chart)

markdown += chart_markdown

with open(".github/workflows/benchmark_comment_markdown.md", "w") as f:
    f.write(markdown)
