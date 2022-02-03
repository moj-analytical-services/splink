from pyspark.sql.dataframe import DataFrame
from typing import Union, List
from .model import Model

from pyspark.sql.functions import expr

from splink.vertically_concat import (
    vertically_concatenate_datasets,
)

graphframes_installed = True
try:
    from graphframes import GraphFrame
except ImportError:
    graphframes_installed = False


graphframes_py_missing_message = """
The python package `graphframes` is required to use this function
but is not installed.

For Spark 2.4.5, you can `pip install graphframes=0.6.0`
or download from https://github.com/graphframes/graphframes/tags

For Spark >=3.0.0, the package version you need is not
available from PyPi, and you should download it from
https://github.com/graphframes/graphframes/releases
"""

graphframes_jar_missing_message = """
To use this function, Spark needs access to the `graphframes` jar.

For Spark 2.4.5 suggested code is:

from pyspark.sql import SparkSession

spark = (SparkSession
   .builder
   .appName("my_app")
   .config('spark.jars.packages', 'graphframes:graphframes:0.6.0-spark2.3-s_2.11')
   .getOrCreate()
   )

spark.sparkContext.setCheckpointDir("graphframes_tempdir/")

# Alternatively if no internet access you need to download the jar AND its dependencies and point Spark to the
# location of the jars in your filesystem:
# config('spark.driver.extraClassPath', 'jars/graphframes-0.6.0-spark2.3-s_2.11.jar,jars/scala-logging-api_2.11-2.1.2.jar,jars/scala-logging-slf4j_2.11-2.1.2.jar') # Spark 2.x only
# config('spark.jars', 'jars/graphframes-0.6.0-spark2.3-s_2.11.jar,jars/scala-logging-api_2.11-2.1.2.jar,jars/scala-logging-slf4j_2.11-2.1.2.jar')

Note extraClassPath is needed on spark 2.x only.

You can find these jars, for example, here https://github.com/moj-analytical-services/splink_graph/tree/master/jars

You can find a list of jars corresponding to different versions of Spark here:
https://mvnrepository.com/artifact/graphframes/graphframes?repo=spark-packages

More info on adding jars to Spark here:
https://spark.apache.org/docs/latest/configuration.html#runtime-environment
"""


def _check_graphframes_installation(spark):

    if not graphframes_installed:
        raise Exception(graphframes_py_missing_message)

    all_config = spark.sparkContext.getConf().getAll()

    config_keys = [
        "spark.submit.pyFiles",
        "spark.repl.local.jars",
        "spark.jars.packages",
        "spark.jars",
        "spark.app.initial.file.urls",
        "spark.files",
        "spark.app.initial.jar.urls",
    ]

    graphframe_jar_registered = False
    for (key, value) in all_config:
        if key in config_keys:
            if "graphframes" in value:
                graphframe_jar_registered = True
                
    databricks_backend = False
    for key, value in all_config:
        if "databricks" in key:
            databricks_backend = True

    if not graphframe_jar_registered:
        if not databricks_backend:
            raise Exception(graphframes_jar_missing_message)

    from pyspark.sql import Row

    errored = False
    try:
        n = spark.createDataFrame([Row(id=1)])
        e = spark.createDataFrame([Row(src=1, dst=2)])
        g = GraphFrame(n, e)
        cc = g.connectedComponents()
    except Exception as e:
        error_string = e.__str__()
        errored = True
    if errored:
        raise Exception(
            "There's something wrong with GraphFrames\n"
            "Graphframes is an external library you need to \n"
            "install \n"
            "See quickstart here: https://graphframes.github.io/graphframes/docs/_site/quick-start.html \n"
            "Either it's not installed or it is not \n"
            "working as expected \n\n" + error_string
        )


def _colname_from_threshold(threshold_value):
    v = threshold_value
    return f"cluster_{v:.5g}".replace(".", "_").replace("-", "neg")


def _threshold_values_to_dict(threshold_values):
    if type(threshold_values) is dict:
        return threshold_values
    if type(threshold_values) is list:
        res = {}
        for v in threshold_values:
            res[_colname_from_threshold(v)] = v
        return res

    if type(threshold_values) is float:
        res = {}
        v = threshold_values
        res[_colname_from_threshold(v)] = v
        return res


def clusters_at_thresholds(
    df_of_dfs_nodes: Union[DataFrame, List[DataFrame]],
    df_edges: DataFrame,
    threshold_values: Union[float, list, dict],
    model: Model,
    join_node_details: bool = True,
    check_graphframes_installation: bool = True,
    score_colname: str = "match_probability",
):
    """Generated a table of clusters at one or more threshold_values
    from a table of scored edges (scored pairwise comparisons)

    Args:
        df_of_dfs_nodes (Union[DataFrame, List[DataFrame]]): Dataframe or Dataframes of nodes (original records
            from which pairwise comparisons  are derived).  If the link_type is `dedupe_only`, this will be a
            single dataframe.  If the link_type is `link_and_dedupe` or `link_only`, this will be a list of dataframes.
            The provided dataframes should be the same as provided to Splink().
        df_edges (DataFrame): Dataframe of edges (pairwise record comparisons with scores)
        threshold_values (Union[float, list, dict]): Threshold values of the match probability (or score_colname)
            above which pairwise comparisons are considered to be a match.  There are three options:
            1. A single float value.  Cluster colname will be 'cluster'
            2. An array of float values.  Clustering will be compled for each value, with colname cluster_0_99
            for a threshold value of 0.99 etc.
            3. A dictionary of threshold values.  Dictionary keys will be used for the cluster columns names.
        model (Model): The Splink Model object
        join_node_details (bool, optional): If true, retain the columns from df_nodes in cluster table.
            Defaults to True.
        check_graphframes_installation (bool, optional): Perform checks to see if the graphframes
            installation has been completed before execution. Defaults to True.
        score_colname (str, optional): Score colname. Defaults to "match_probability".  Could also be e.g.
            'match_weight'

    Returns:
        DataFrame: clustered DataFrame
    """

    # dfs is a list of dfs irrespective of whether input was a df or list of dfs
    if type(df_of_dfs_nodes) == DataFrame:
        dfs = [df_of_dfs_nodes]
    else:
        dfs = df_of_dfs_nodes

    spark = dfs[0].sql_ctx.sparkSession
    df_nodes = vertically_concatenate_datasets(dfs)

    if check_graphframes_installation:
        _check_graphframes_installation(spark)

    # Convert threshold_values to a dictionary
    threshold_values = _threshold_values_to_dict(threshold_values)

    # Define a unique id column
    settings_obj = model.current_settings_obj

    uid_colname = settings_obj["unique_id_column_name"]

    if settings_obj["link_type"] == "dedupe_only":
        uid_df_nodes_col = uid_colname
        uid_col_l = f"{uid_colname}_l"
        uid_col_r = f"{uid_colname}_r"
    else:
        source_ds_colname = settings_obj["source_dataset_column_name"]
        uid_df_nodes_col = f"concat({source_ds_colname}, '-__-',{uid_colname})"
        uid_col_l = f"concat({source_ds_colname}_l, '-__-',{uid_colname}_l)"
        uid_col_r = f"concat({source_ds_colname}_r, '-__-',{uid_colname}_r)"

    df_nodes.createOrReplaceTempView("df_nodes")
    df_edges.createOrReplaceTempView("df_edges")

    sql = f"""
    select {uid_df_nodes_col} as id
    from df_nodes
    """
    df_nodes_id = spark.sql(sql)

    cc_thresholds = {}
    for colname, value in threshold_values.items():
        sql = f"""
        select
            {uid_col_l} as src,
            {uid_col_r} as dst
        from df_edges
        where {score_colname} > {value}
        """
        edges_above_thres = spark.sql(sql)
        g = GraphFrame(df_nodes_id, edges_above_thres)
        cc = g.connectedComponents()
        cc_thresholds[colname] = cc

    for cc_col_name, cc in cc_thresholds.items():
        df_nodes_id = df_nodes_id.join(cc, on=["id"], how="left")
        df_nodes_id = df_nodes_id.withColumnRenamed("component", cc_col_name)

    cluster_colnames = cc_thresholds.keys()
    if join_node_details:
        df_nodes_id.createOrReplaceTempView("df_nodes_id")

        df_nodes = df_nodes.withColumn("___id__", expr(uid_df_nodes_col))
        df_nodes.createOrReplaceTempView("df_nodes")

        cluster_sel = ", ".join(cluster_colnames)

        sql = f"""
        select {cluster_sel}, df_nodes.*
        from df_nodes
        left join df_nodes_id
        on df_nodes_id.id = df_nodes.___id__

        """
        df_nodes = spark.sql(sql)
        df_nodes = df_nodes.drop("___id__")
    else:
        df_nodes = df_nodes_id

    return df_nodes
