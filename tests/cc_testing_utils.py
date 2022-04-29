import networkx as nx
from networkx.algorithms import connected_components as cc_nx
import pandas as pd
import random

from splink.duckdb.duckdb_linker import DuckDBLinker, DuckDBLinkerDataFrame


def generate_random_graph(graph_size, seed=None):
    if not seed:
        seed = random.randint(5, 1000000)

    print(f"Seed set to {seed}")
    graph = nx.fast_gnp_random_graph(graph_size, 0.001, seed=seed, directed=False)
    return graph


def register_cc_df(G):

    from tests.basic_settings import get_settings_dict
    settings_dict = get_settings_dict()

    df = nx.to_pandas_edgelist(G)
    df.columns = ["unique_id_l", "unique_id_r"]
    df = pd.concat([
        pd.DataFrame({
            "unique_id_l": G.nodes,
            "unique_id_r": G.nodes}
        ),
        df
    ])

    # boot up our linker
    table_name = "__splink__df_predict_graph"
    # this registers our table under __splink__df__{table_name}
    # but our cc function actively looks for "__splink__df_predict"
    linker = DuckDBLinker(settings_dict, input_tables={table_name: df})

    # re-register under our required name to run the CC function
    linker.con.register(table_name, df)

    # add our prediction df to our list of created tables
    predict_df = DuckDBLinkerDataFrame(table_name, table_name, linker)
    linker.names_of_tables_created_by_splink = [predict_df]

    return linker


def run_cc_implementation(linker):

    # finally, run our connected components linker
    linker.run_connected_components()

    return linker
