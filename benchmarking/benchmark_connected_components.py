# python3 -m pytest benchmarking/benchmark_connected_components.py
from tests.cc_testing_utils import (
    benchmark_cc_implementation,
    generate_random_graph,
    register_cc_df,
)

###############################################################################
# Performance Testing
###############################################################################

# set a seed for consistent benchmarking
seed = 5


def test_500_node_performance(benchmark):
    g = generate_random_graph(graph_size=500, seed=seed)
    linker = register_cc_df(g)

    benchmark.pedantic(
        benchmark_cc_implementation,
        args=(linker,),
        rounds=10,
        iterations=10,
        warmup_rounds=0,
    )


def test_10000_node_performance(benchmark):
    g = generate_random_graph(graph_size=10000, seed=seed)
    linker = register_cc_df(g)

    benchmark.pedantic(
        benchmark_cc_implementation,
        args=(linker,),
        rounds=10,
        iterations=10,
        warmup_rounds=0,
    )


def test_100000_node_performance(benchmark):
    g = generate_random_graph(graph_size=100000, seed=seed)
    linker = register_cc_df(g)

    benchmark.pedantic(
        benchmark_cc_implementation,
        args=(linker,),
        rounds=10,
        iterations=2,
        warmup_rounds=0,
    )
