import os
import json

import pandas as pd
from pandas import json_normalize
import altair as alt
import lzstring


def append_output_to_timeseries(base_path="benchmarking"):

    with open(os.path.join(base_path, "time_series.json")) as f:
        time_series_data = []
        for line in f:
            time_series_data.append(json.loads(line))

    with open(os.path.join(base_path, "output.json")) as f:
        new_data = json.load(f)

    time_series_data.append(new_data)

    with open(os.path.join(base_path, "time_series.json"), "w") as f:

        for entry in time_series_data:
            json.dump(entry, f)
            f.write("\n")

    return new_data


def load_timeseries(base_path="benchmarking"):
    df = json_normalize(
        pd.Series(open(os.path.join(base_path, "time_series.json")).readlines()).apply(
            json.loads
        )
    )
    df2 = df.explode("benchmarks").reset_index()

    df3 = pd.json_normalize(df2["benchmarks"])
    df4 = df2.join(df3)

    cols = [c.replace(".", "_") for c in df4.columns]
    df4.columns = cols
    return df4


def format_timeseries_data_table(df_timeseries):
    df_timeseries["datetime"] = pd.to_datetime(df_timeseries["datetime"])

    df_timeseries["date"] = df_timeseries["datetime"].dt.strftime("%Y-%m-%d")

    df_timeseries["time"] = df_timeseries["datetime"].dt.strftime("%H:%M:%S")

    cols = [
        "name",
        "date",
        "time",
        "datetime",
        "stats_mean",
        "stats_min",
        "commit_info_branch",
        "commit_info_id",
        "machine_info_cpu_brand_raw",
        "machine_info_cpu_hz_actual_friendly",
    ]
    df_timeseries = df_timeseries[cols].copy()
    df_timeseries["commit_hash"] = df_timeseries["commit_info_id"].str[:7]
    df_timeseries = df_timeseries.rename(columns={"name": "name_of_test"})

    return df_timeseries


def get_latest_on_branch(df, branch, cpu):
    f1 = df["commit_info_branch"] == branch
    f2 = df["machine_info_cpu_brand_raw"] == cpu
    df = df[f1 & f2]
    df = df.sort_values("datetime", ascending=True)
    return df.tail(1)


def get_latest(df, cpu):
    f1 = df["machine_info_cpu_brand_raw"] == cpu
    df = df[f1]
    df = df.sort_values("datetime", ascending=True)
    return df.tail(1)


def get_markdown_tables(timeseries_df, cpu):
    def add_to_markdown(df):

        df1 = get_latest_on_branch(df, "splink3", cpu)
        df2 = get_latest(df, cpu)

        table = pd.concat([df1, df2])
        len1 = len(table)
        table = table.drop_duplicates("commit_info_branch")
        len2 = len(table)

        # If duplicated, then latest on main is latest, so test was not run on this PR
        if len2 < len1:
            return ""

        if len(table) == 0:
            return ""

        prop_change = ""
        if len(table) == 2:
            previous_min = table["stats_min"].iloc[0]
            this_min = table["stats_min"].iloc[0]
            prop_change = (this_min - previous_min) / previous_min
            prop_change = prop_change - 1
            prop_change = f"Percentage change: {prop_change:.1%}\n"

        name = table["name_of_test"].iloc[0]
        table = table.drop("name_of_test", axis=1)
        table = table.drop("datetime", axis=1)
        md = table.to_markdown()
        return f"### Test: {name}\n\n{prop_change}{md}\n\n"

    mds = timeseries_df.groupby("name_of_test").apply(add_to_markdown)
    return " ".join(mds)


def get_chart(timeseries_df):

    df = timeseries_df
    df["machine_info_cpu_brand_raw"] = df["machine_info_cpu_brand_raw"].str.replace(
        "Intel(R)", "", regex=False
    )

    df["machine_info_cpu_brand_raw"] = df["machine_info_cpu_brand_raw"].str.replace(
        "Xeon(R)", "", regex=False
    )

    df["machine_info_cpu_brand_raw"] = df["machine_info_cpu_brand_raw"].str.replace(
        "Core(TM)", "", regex=False
    )

    df["machine_info_cpu_brand_raw"] = df["machine_info_cpu_brand_raw"].str.replace(
        "Platinum", "", regex=False
    )

    df["machine_info_cpu_brand_raw"] = df["machine_info_cpu_brand_raw"].str.strip()

    c = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X("commit_hash:N", sort=["datetime"]),
            y=alt.Y("stats_min", axis=alt.Axis(title=None)),
            row=alt.Row(
                "name_of_test",
                title=None,
                header=alt.Header(labelAngle=0, labelAlign="left"),
            ),
            color="machine_info_cpu_brand_raw",
            tooltip=[
                "name_of_test",
                "stats_min",
                "commit_info_branch",
                "date",
                "time",
                "machine_info_cpu_brand_raw",
            ],
        )
        .properties(
            height=100,
            title={
                "text": "Runtime of benchmarks by commit",
                "subtitle": "Hover over lines for tooltip",
            },
        )
        .configure_title(align="center", anchor="middle")
        .resolve_scale(y="independent")
    )
    return c


def chart_to_markdown(alt_chart):
    cj = alt_chart.to_json()

    x = lzstring.LZString()
    compressed = x.compressToEncodedURIComponent(cj)

    return f"**Click [here](https://vega.github.io/editor/#/url/vega-lite/{compressed}) for vega lite time series charts**"
