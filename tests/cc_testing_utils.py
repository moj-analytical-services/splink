from copy import deepcopy
import networkx as nx
from networkx.algorithms import connected_components as cc_nx
import pandas as pd
import random

from splink.duckdb.duckdb_linker import DuckDBLinker, DuckDBLinkerDataFrame
from splink.connected_components import solve_connected_components


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
    df = pd.concat([pd.DataFrame({"unique_id_l": G.nodes, "unique_id_r": G.nodes}), df])

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

    return predict_df


def run_cc_implementation(splink_df, batching=1):

    # finally, run our connected components algorithm
    return solve_connected_components(
        splink_df.duckdb_linker, splink_df,
        batching=batching
    ).as_pandas_dataframe()


def benchmark_cc_implementation(linker_df, batching=1):

    # add a schema so we don't need to re-register our df
    linker_df.duckdb_linker.con.execute(
        """
        create schema if not exists con_comp;
        set schema 'con_comp';
        """
    )

    df = run_cc_implementation(linker_df, batching)
    linker_df.duckdb_linker.con.execute("drop schema con_comp cascade")

    return df


def networkx_solve(G):
    rows = []
    for cc in cc_nx(G):
        m = min(list(cc))
        for n in cc:
            row = {"node_id": n, "representative": m}
            rows.append(row)
    return pd.DataFrame(rows)


def check_df_equality(df1, df2):
    """
    Test if two dataframes are equal
    :param df1: First dataframe
    :param df2: Second dataframe
    :return: True if equal, False if not
    """
    if df1.shape != df2.shape:
        return False
    if df1.columns.tolist() != df2.columns.tolist():
        return False
    if df1.dtypes.tolist() != df2.dtypes.tolist():
        return False
    for col in df1.columns:
        if df1[col].tolist() != df2[col].tolist():
            return False
    return True
