from typing import Optional

from splink.internals.connected_components import solve_connected_components
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.input_column import InputColumn
from splink.internals.splink_dataframe import SplinkDataFrame


def cluster_pairwise_predictions_at_threshold(
    nodes: AcceptableInputTableType,
    edges: AcceptableInputTableType,
    db_api: DatabaseAPISubClass,
    node_id_column_name: str,
    edge_id_column_name_left: Optional[str] = None,
    edge_id_column_name_right: Optional[str] = None,
    threshold_match_probability: Optional[float] = None,
) -> SplinkDataFrame:
    nodes_sdf = db_api.register_table(nodes, "__splink__df_nodes", overwrite=True)
    edges_sdf = db_api.register_table(edges, "__splink__df_edges", overwrite=True)

    if not edge_id_column_name_left:
        edge_id_column_name_left = InputColumn(
            node_id_column_name,
            sqlglot_dialect_str=db_api.sql_dialect.sqlglot_dialect,
        ).name_l

    if not edge_id_column_name_right:
        edge_id_column_name_right = InputColumn(
            node_id_column_name,
            sqlglot_dialect_str=db_api.sql_dialect.sqlglot_dialect,
        ).name_r

    cc = solve_connected_components(
        nodes_table=nodes_sdf,
        edges_table=edges_sdf,
        node_id_column_name=node_id_column_name,
        edge_id_column_name_left=edge_id_column_name_left,
        edge_id_column_name_right=edge_id_column_name_right,
        db_api=db_api,
        threshold_match_probability=threshold_match_probability,
    )
    cc.metadata["threshold_match_probability"] = threshold_match_probability
    return cc
