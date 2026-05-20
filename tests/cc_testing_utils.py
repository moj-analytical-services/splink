import random

import networkx as nx
from networkx.algorithms import connected_components as cc_nx

from splink.internals.clustering import cluster_pairwise_predictions_at_threshold
from splink.internals.duckdb.database_api import DuckDBAPI


def generate_random_graph(graph_size, density=0.001, seed=None):
    if not seed:
        seed = random.randint(5, 1000000)

    graph = nx.fast_gnp_random_graph(graph_size, density, seed=seed, directed=False)
    return graph


def nodes_and_edges_from_graph(G):
    edges = nx.to_edgelist(G)
    nodes = {"unique_id": [*G.nodes]}

    return nodes, [
        {"unique_id_l": node_l, "unique_id_r": node_r} for node_l, node_r, _ in edges
    ]


def run_cc_implementation(nodes, edges):
    # finally, run our connected components algorithm
    db_api = DuckDBAPI()
    nodes_sdf = db_api.register(nodes)
    edges_sdf = db_api.register(edges)
    cc = cluster_pairwise_predictions_at_threshold(
        nodes=nodes_sdf,
        edges=edges_sdf,
        db_api=db_api,
        node_id_column_name="unique_id",
        edge_id_column_name_left="unique_id_l",
        edge_id_column_name_right="unique_id_r",
        threshold_match_probability=None,
    )

    return cc.query_sql(
        """
        SELECT
            unique_id AS node_id,
            cluster_id AS representative,
        FROM
            {this}
        ORDER BY
            node_id, representative
        """
    )


def networkx_solve(G):
    rows = []
    for cc in cc_nx(G):
        m = min(list(cc))
        for n in cc:
            row = {"node_id": n, "representative": m}
            rows.append(row)

    return sorted(rows, key=lambda node: (node["node_id"], node["representative"]))
