import random

import networkx as nx
import pandas as pd
from networkx.algorithms import connected_components as cc_nx

from splink.internals.clustering import cluster_pairwise_predictions_at_threshold
from splink.internals.duckdb.database_api import DuckDBAPI


def generate_random_graph(graph_size, density=0.001, seed=None):
    if not seed:
        seed = random.randint(5, 1000000)

    graph = nx.fast_gnp_random_graph(graph_size, density, seed=seed, directed=False)
    return graph


def nodes_and_edges_from_graph(G):
    edges = nx.to_pandas_edgelist(G)
    edges.columns = ["unique_id_l", "unique_id_r"]

    nodes = pd.DataFrame({"unique_id": G.nodes})

    return nodes, edges


def run_cc_implementation(nodes, edges):
    # finally, run our connected components algorithm
    db_api = DuckDBAPI()
    cc = cluster_pairwise_predictions_at_threshold(
        nodes=nodes,
        edges=edges,
        db_api=db_api,
        node_id_column_name="unique_id",
        edge_id_column_name_left="unique_id_l",
        edge_id_column_name_right="unique_id_r",
        threshold_match_probability=None,
    ).as_pandas_dataframe()

    cc = cc.rename(columns={"unique_id": "node_id", "cluster_id": "representative"})
    cc = cc[["node_id", "representative"]]
    cc.sort_values(by=["node_id", "representative"], inplace=True)
    return cc


def networkx_solve(G):
    rows = []
    for cc in cc_nx(G):
        m = min(list(cc))
        for n in cc:
            row = {"node_id": n, "representative": m}
            rows.append(row)
    return pd.DataFrame(rows).sort_values(by=["node_id", "representative"])
