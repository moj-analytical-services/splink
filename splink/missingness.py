from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, when

from .charts import load_chart_definition, altair_if_installed_else_json

import pandas as pd


def missingness_chart(df: DataFrame):
    """Produce bar chart of missingness in standardised nodes
    Args:
        df (DataFrame): Input Spark dataframe
    Returns:
        Bar chart of missingness
    """
    # Load JSON definition of missingness chart
    chart_path = "missingness_chart_def.json"
    missingness_chart_def = load_chart_definition(chart_path)

    # Data for plot
    # Count and percentage of nulls in each columns as pandas dataframe
    df_nulls = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    pd_nulls = df_nulls.toPandas()
    pd_nulls = pd.melt(pd_nulls)

    record_count = df.count()
    pd_nulls["percentage"] = pd_nulls["value"] / record_count

    # Add data to JSON chart definition
    missingness_chart_def["data"]["values"] = pd_nulls.to_dict("records")

    # Update chart title
    for c in missingness_chart_def["layer"]:
        c["title"] = f"Missingness per column out of {record_count:,.0f} records"

    return altair_if_installed_else_json(missingness_chart_def)
