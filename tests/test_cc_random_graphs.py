# python3 -m pytest tests/test_cc_random_graphs.py
import pytest

from tests.cc_testing_utils import (
    check_df_equality,
    generate_random_graph,
    networkx_solve,
    register_cc_df,
    run_cc_implementation,
)

###############################################################################
# Accuracy Testing
###############################################################################


@pytest.mark.parametrize("execution_number", range(20))
def test_small_erdos_renyi_graph(execution_number):
    g = generate_random_graph(graph_size=500)
    linker, predict_df = register_cc_df(g)

    assert check_df_equality(
        run_cc_implementation(linker, predict_df).sort_values(
            by=["node_id", "representative"]
        ),
        networkx_solve(g).sort_values(by=["node_id", "representative"]),
    )


@pytest.mark.skip(reason="Slow")
@pytest.mark.parametrize("execution_number", range(10))
def test_medium_erdos_renyi_graph(execution_number):
    g = generate_random_graph(graph_size=10000)
    linker, predict_df = register_cc_df(g)

    assert check_df_equality(
        run_cc_implementation(linker, predict_df).sort_values(
            by=["node_id", "representative"]
        ),
        networkx_solve(g).sort_values(by=["node_id", "representative"]),
    )


@pytest.mark.skip(reason="Slow")
@pytest.mark.parametrize("execution_number", range(2))
def test_large_erdos_renyi_graph(execution_number):
    g = generate_random_graph(graph_size=100000)
    linker, predict_df = register_cc_df(g)

    assert check_df_equality(
        run_cc_implementation(linker, predict_df).sort_values(
            by=["node_id", "representative"]
        ),
        networkx_solve(g).sort_values(by=["node_id", "representative"]),
    )
