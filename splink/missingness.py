from splink.charts import load_chart_definition, altair_if_installed_else_json

from pyspark.sql.functions import col, count, when

import pandas as pd


def missingness_chart(df):
    """Produce histogram of missingness in standardised nodes
    Args:
        df (DataFrame): Input Spark dataframe
    Returns:
        
    """
    
    # Load JSON chart definition
    chart_path = "missingness_chart_def.json"
    missingness_chart_def = load_chart_definition(chart_path)
    
    # Count and percentage of nulls in each columns as pandas dataframe
    df_nulls = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    pd_nulls = df_nulls.toPandas()
    pd_nulls = pd.melt(pd_nulls)
    
    pd_nulls['record_count'] = df.count()
    pd_nulls['percentage'] = round(pd_nulls['value']*100/ pd_nulls['record_count'], 1)
    pd_nulls['percentage'] = pd_nulls['percentage'].astype(str) + "%"
    
    return altair_if_installed_else_json("missingness_chart_def.json")