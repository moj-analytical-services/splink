from typing import Optional

from splink.internals.connected_components import solve_connected_components
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.input_column import InputColumn
from splink.internals.misc import ascii_uid
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
    """Clusters the pairwise match predictions into groups of connected records using
    the connected components graph clustering algorithm.

    Records with an estimated match probability at or above threshold_match_probability
    are considered to be a match (i.e. they represent the same entity).

    If no match probability column is provided, it is assumed that all edges
    (comparison) are a match.

    If your node and edge column names follow Splink naming conventions, then you can
    omit edge_id_column_name_left and edge_id_column_name_right. For example, if you
    have a table of nodes with a column `unique_id`, it would be assumed that the
    edge table has columns `unique_id_l` and `unique_id_r`.

    Args:
        nodes (AcceptableInputTableType): The table containing node information
        edges (AcceptableInputTableType): The table containing edge information
        db_api (DatabaseAPISubClass): The database API to use for querying
        node_id_column_name (str): The name of the column containing node IDs
        edge_id_column_name_left (Optional[str]): The name of the column containing
            left edge IDs. If not provided, assumed to be f"{node_id_column_name}_l"
        edge_id_column_name_right (Optional[str]): The name of the column containing
            right edge IDs. If not provided, assumed to be f"{node_id_column_name}_r"
        threshold_match_probability (Optional[float]): Pairwise comparisons with a
            match_probability at or above this threshold are matched

    Returns:
        SplinkDataFrame: A SplinkDataFrame containing a list of all IDs, clustered
            into groups based on the desired match threshold.

    Examples:
        ```python
        from splink import DuckDBAPI
        from splink.clustering import cluster_pairwise_predictions_at_threshold

        db_api = DuckDBAPI()

        nodes = [
            {"my_id": 1},
            {"my_id": 2},
            {"my_id": 3},
            {"my_id": 4},
            {"my_id": 5},
            {"my_id": 6},
        ]

        edges = [
            {"n_1": 1, "n_2": 2, "match_probability": 0.8},
            {"n_1": 3, "n_2": 2, "match_probability": 0.9},
            {"n_1": 4, "n_2": 5, "match_probability": 0.99},
        ]

        cc = cluster_pairwise_predictions_at_threshold(
            nodes,
            edges,
            node_id_column_name="my_id",
            edge_id_column_name_left="n_1",
            edge_id_column_name_right="n_2",
            db_api=db_api,
            threshold_match_probability=0.5,
        )

        cc.as_duckdbpyrelation()
        ```
    """

    uid = ascii_uid(8)

    if isinstance(nodes, SplinkDataFrame):
        nodes_sdf = nodes
    else:
        nodes_sdf = db_api.register_table(nodes, f"__splink__df_nodes_{uid}")

    if isinstance(edges, SplinkDataFrame):
        edges_sdf = edges
    else:
        edges_sdf = db_api.register_table(edges, f"__splink__df_edges_{uid}")

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
