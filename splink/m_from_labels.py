from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit
from splink.vertically_concat import vertically_concatenate_datasets
from splink.lower_id_on_lhs import lower_id_to_left_hand_side
from splink.blocking import _get_columns_to_retain_blocking, sql_gen_comparison_columns
from splink.settings import Settings
from splink.blocking import block_using_rules
from splink.gammas import add_gammas
from splink.maximisation_step import run_maximisation_step
from splink.model import Model
from splink.cluster import _check_graphframes_installation
from splink.default_settings import complete_settings_dict


def estimate_m_from_labels(
    settings: dict,
    df_or_dfs: DataFrame,
    labels: DataFrame,
    use_connected_components,
    fix_m_probabilities=False,
):
    """Estimate m values from labels

    Args:
        settings (dict): splink settings dictionary
        df_or_dfs (DataFrame): (DataFrame or list of DataFrames, optional):
        labels (DataFrame): Labelled data.
            For link or link and dedupe, should have columns:
            'source_dataset_l', 'unique_id_l', 'source_dataset_r', and 'unique_id_r'
            For dedupe only, only needs 'unique_id_l' and 'unique_id_r' columns
        use_connected_components (bool, optional): Whether to use the connected components approach.
            Defaults to True.  Described here: https://github.com/moj-analytical-services/splink/issues/245
        fix_m_probabilities (bool, optional): If True, output comparison column settings will have
            fix_u_probabilities set to True.  Defaults to False.

    """

    # dfs is a list of dfs irrespective of whether input was a df or list of dfs
    if type(df_or_dfs) == DataFrame:
        dfs = [df_or_dfs]
    else:
        dfs = df_or_dfs

    spark = dfs[0].sql_ctx.sparkSession

    if use_connected_components:
        _check_graphframes_installation(spark)

    df_nodes = vertically_concatenate_datasets(dfs)

    settings_complete = complete_settings_dict(settings, spark)
    if settings_complete["link_type"] == "dedupe_only":
        use_source_dataset = False
    else:
        use_source_dataset = True

    source_dataset_colname = settings_complete["source_dataset_column_name"]
    uid_colname = settings_complete["unique_id_column_name"]

    if use_connected_components:
        df_gammas = _get_comparisons_using_connected_components(
            df_nodes,
            labels,
            settings_complete,
            use_source_dataset,
            source_dataset_colname,
            uid_colname,
        )
    else:
        df_gammas = _get_comparisons_using_joins(
            df_nodes,
            labels,
            settings_complete,
            use_source_dataset,
            source_dataset_colname,
            uid_colname,
        )

    df_e = df_gammas.withColumn("match_probability", lit(1.0))

    model = Model(settings_complete, spark)
    run_maximisation_step(df_e, model, spark)

    settings_with_m_dict = model.current_settings_obj.settings_dict

    # We want to add m probabilities from these estimates to the settings_with_u object
    settings_obj = Settings(settings)

    settings_obj.overwrite_m_u_probs_from_other_settings_dict(
        settings_with_m_dict, overwrite_u=False
    )

    for cc in settings_obj.comparison_columns_list:
        if fix_m_probabilities:
            cc.fix_m_probabilities = True

    return settings_obj.settings_dict


def _get_comparisons_using_connected_components(
    df_nodes,
    df_labels,
    settings_complete,
    use_source_dataset,
    source_dataset_colname,
    uid_colname,
):
    from graphframes import GraphFrame

    spark = df_nodes.sql_ctx.sparkSession

    if use_source_dataset:
        uid_node = f"concat({source_dataset_colname}, '-__-',{uid_colname}) as id"
        uid_r = f"concat({source_dataset_colname}_l, '-__-',{uid_colname}_l) as src"
        uid_l = f"concat({source_dataset_colname}_r, '-__-',{uid_colname}_r) as dst"
    else:
        uid_node = f"{uid_colname} as id"
        uid_r = f"{uid_colname}_l as src"
        uid_l = f"{uid_colname}_r as dst"

    cc_nodes = df_nodes.selectExpr(uid_node)
    edges = df_labels.selectExpr(uid_l, uid_r)
    g = GraphFrame(cc_nodes, edges)
    g = g.dropIsolatedVertices()
    cc = g.connectedComponents()

    df_nodes.createOrReplaceTempView("df_nodes")
    cc.createOrReplaceTempView("cc")

    if use_source_dataset:
        join_col_expr = (
            f"concat(df_nodes.{source_dataset_colname}, '-__-',df_nodes.{uid_colname})"
        )
    else:
        join_col_expr = f"df_nodes.{uid_colname}"

    sql = f"""
    select df_nodes.*, cc.component as cluster
    from df_nodes
    inner join cc
    on cc.id = {join_col_expr}
    """

    df_with_cluster = spark.sql(sql)

    settings_complete["blocking_rules"] = ["l.cluster = r.cluster"]

    df_comparison = block_using_rules(settings_complete, df_with_cluster, spark)
    df_gammas = add_gammas(df_comparison, settings_complete, spark)

    return df_gammas


def _get_comparisons_using_joins(
    df_nodes,
    df_labels,
    settings_complete,
    use_source_dataset,
    source_dataset_colname,
    uid_colname,
):
    spark = df_nodes.sql_ctx.sparkSession
    df_labels = lower_id_to_left_hand_side(
        df_labels, source_dataset_colname, uid_colname
    )

    df_nodes.createOrReplaceTempView("df_nodes")
    df_labels.createOrReplaceTempView("df_labels")

    columns_to_retain = _get_columns_to_retain_blocking(settings_complete, df_nodes)

    sql_select_expr = sql_gen_comparison_columns(columns_to_retain)

    if use_source_dataset:

        sql = f"""
        select {sql_select_expr}, '0' as match_key
        from df_nodes as l
        inner join df_labels
            on l.{source_dataset_colname} = df_labels.{source_dataset_colname}_l and l.{uid_colname} = df_labels.{uid_colname}_l
        inner join df_nodes as r
            on r.{source_dataset_colname} = df_labels.{source_dataset_colname}_r and r.{uid_colname} = df_labels.{uid_colname}_r
        """
    else:
        sql = f"""
        select {sql_select_expr}, '0' as match_key
        from df_nodes as l
        inner join df_labels
            on l.{uid_colname} = df_labels.{uid_colname}_l
        inner join df_nodes as r
            on r.{uid_colname} = df_labels.{uid_colname}_r
        """

    df_comparison = spark.sql(sql)
    df_gammas = add_gammas(df_comparison, settings_complete, spark)
    return df_gammas
