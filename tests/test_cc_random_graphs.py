# python3 -m pytest tests/test_cc_random_graphs.py
import pytest

from tests.cc_testing_utils import (
    generate_random_graph,
    networkx_solve,
    nodes_and_edges_from_graph,
    run_cc_implementation,
)


@pytest.mark.parametrize("execution_number", range(20))
def test_small_erdos_renyi_graph(execution_number):
    g = generate_random_graph(graph_size=500)
    df_nodes, df_edges = nodes_and_edges_from_graph(g)

    cc_df = run_cc_implementation(df_nodes, df_edges)
    nx_df = networkx_solve(g)

    assert (cc_df.values == nx_df.values).all()


@pytest.mark.skip(reason="Slow")
@pytest.mark.parametrize("execution_number", range(10))
def test_medium_erdos_renyi_graph(execution_number):
    g = generate_random_graph(graph_size=10000)
    df_nodes, df_edges = nodes_and_edges_from_graph(g)

    cc_df = run_cc_implementation(df_nodes, df_edges)
    nx_df = networkx_solve(g)
    assert (cc_df.values == nx_df.values).all()


@pytest.mark.skip(reason="Slow")
@pytest.mark.parametrize("execution_number", range(2))
def test_large_erdos_renyi_graph(execution_number):
    g = generate_random_graph(graph_size=100000)
    df_nodes, df_edges = nodes_and_edges_from_graph(g)

    cc_df = run_cc_implementation(df_nodes, df_edges)
    nx_df = networkx_solve(g)
    assert (cc_df.values == nx_df.values).all()
