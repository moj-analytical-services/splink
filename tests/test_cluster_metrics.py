import pandas as pd
from pandas.testing import assert_frame_equal
from pytest import approx

from splink.duckdb.duckdb_comparison_library import (
    exact_match,
)
from splink.duckdb.linker import DuckDBLinker

from .decorator import mark_with_dialects_excluding

df_1 = [
    {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
    {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
    {"unique_id": 3, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
]

df_2 = [
    {"unique_id": 1, "first_name": "Bob", "surname": "Ray", "dob": "1999-09-22"},
    {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
]

df_1 = pd.DataFrame(df_1)
df_2 = pd.DataFrame(df_2)


def test_size_density_dedupe():
    settings = {
        "probability_two_random_records_match": 0.01,
        "link_type": "dedupe_only",
        "comparisons": [
            exact_match("first_name"),
            exact_match("surname"),
            exact_match("dob"),
        ],
    }
    linker = DuckDBLinker(df_1, settings)

    df_predict = linker.predict()
    df_clustered = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.9)

    df_result = linker._compute_graph_metrics(
        df_predict, df_clustered, threshold_match_probability=0.9
    ).clusters.as_pandas_dataframe()
    # not testing this here - it's not relevant for small clusters anyhow
    del df_result["cluster_centralisation"]

    data_expected = [
        {"cluster_id": 1, "n_nodes": 1, "n_edges": 0.0, "density": None},
        {"cluster_id": 2, "n_nodes": 2, "n_edges": 1.0, "density": 1.0},
    ]
    df_expected = pd.DataFrame(data_expected)

    assert_frame_equal(df_result, df_expected, check_index_type=False)


def test_size_density_link():
    settings = {
        "probability_two_random_records_match": 0.01,
        "link_type": "link_only",
        "comparisons": [
            exact_match("first_name"),
            exact_match("surname"),
            exact_match("dob"),
        ],
    }
    linker = DuckDBLinker(
        [df_1, df_2], settings, input_table_aliases=["df_left", "df_right"]
    )

    df_predict = linker.predict()
    df_clustered = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.9)

    df_result = (
        linker._compute_graph_metrics(
            df_predict, df_clustered, threshold_match_probability=0.99
        )
        .clusters.as_pandas_dataframe()
        .sort_values(by="cluster_id")
        .reset_index(drop=True)
    )
    del df_result["cluster_centralisation"]

    data_expected = [
        {
            "cluster_id": "df_left-__-1",
            "n_nodes": 1,
            "n_edges": 0.0,
            "density": None,
        },
        {
            "cluster_id": "df_left-__-2",
            "n_nodes": 3,
            "n_edges": 2.0,
            "density": 0.666667,
        },
        {
            "cluster_id": "df_right-__-1",
            "n_nodes": 1,
            "n_edges": 0.0,
            "density": None,
        },
    ]
    df_expected = (
        pd.DataFrame(data_expected).sort_values(by="cluster_id").reset_index(drop=True)
    )

    assert_frame_equal(df_result, df_expected, check_index_type=False)


def make_row(id_l: int, id_r: int, group_id: int, match_probability: float):
    return {
        "unique_id_l": id_l,
        "unique_id_r": id_r,
        "cluster_id": group_id,
        "match_probability": match_probability,
    }


@mark_with_dialects_excluding()
def test_metrics(dialect, test_helpers):
    helper = test_helpers[dialect]
    df_e = pd.DataFrame(
        [
            # group 1
            # 4 nodes, 4 edges
            make_row(1, 2, 1, 0.96),
            make_row(1, 3, 1, 0.98),
            make_row(1, 4, 1, 0.98),
            make_row(2, 4, 1, 0.98),
            # group 2
            # 6 nodes, 5 edges
            make_row(5, 6, 2, 0.96),
            make_row(5, 7, 2, 0.97),
            make_row(5, 9, 2, 0.99),
            make_row(7, 8, 2, 0.96),
            make_row(9, 10, 2, 0.96),
            # group 3
            # 2 nodes, 1 edge
            make_row(11, 12, 3, 0.99),
            # group 4
            # 11 nodes, 19 edges
            make_row(13, 14, 4, 0.99),
            make_row(13, 15, 4, 0.99),
            make_row(13, 16, 4, 0.99),
            make_row(13, 17, 4, 0.99),
            make_row(13, 18, 4, 0.99),
            make_row(13, 19, 4, 0.99),
            make_row(14, 15, 4, 0.99),
            make_row(14, 16, 4, 0.99),
            make_row(15, 16, 4, 0.99),
            make_row(15, 17, 4, 0.99),
            make_row(16, 18, 4, 0.99),
            make_row(16, 20, 4, 0.99),
            make_row(17, 21, 4, 0.99),
            make_row(18, 19, 4, 0.99),
            make_row(18, 21, 4, 0.99),
            make_row(18, 22, 4, 0.99),
            make_row(20, 22, 4, 0.99),
            make_row(20, 23, 4, 0.99),
            make_row(22, 23, 4, 0.99),
            # edges that don't make the cut
            # these should affect nothing
            make_row(1, 8, None, 0.94),
            make_row(2, 3, None, 0.92),
            make_row(5, 10, None, 0.93),
            make_row(4, 11, None, 0.945),
            make_row(5, 16, None, 0.9),
            make_row(7, 20, None, 0.93),
            make_row(17, 20, None, 0.92),
        ]
    )
    df_c = pd.DataFrame(
        [{"cluster_id": 1, "unique_id": i} for i in range(1, 4 + 1)]
        + [{"cluster_id": 2, "unique_id": i} for i in range(5, 10 + 1)]
        + [{"cluster_id": 3, "unique_id": i} for i in range(11, 12 + 1)]
        + [{"cluster_id": 4, "unique_id": i} for i in range(13, 23 + 1)]
        + [{"cluster_id": 5, "unique_id": i} for i in range(24, 24 + 1)]
    )

    expected_node_degrees = [
        # cluster 1
        # max degree 3
        # centralisation = (1 + 2 + 1)/(3 * 2)
        (1, 3),
        (2, 2),
        (3, 1),
        (4, 2),
        # cluster 2
        # centralisation = (2 + 1 + 2 + 1 + 2)/(5 * 4)
        (5, 3),
        (6, 1),
        (7, 2),
        (8, 1),
        (9, 2),
        (10, 1),
        # cluster 3
        # centralisation = NULL
        (11, 1),
        (12, 1),
        # cluster 4
        # centralisation = (3 + 2 + 1 + 3 + 1 + 4 + 3 + 4 + 3 + 4)/(10*9)
        (13, 6),
        (14, 3),
        (15, 4),
        (16, 5),
        (17, 3),
        (18, 5),
        (19, 2),
        (20, 3),
        (21, 2),
        (22, 3),
        (23, 2),
        # cluster 5
        # centralisation = NULL
        (24, 0),
    ]

    # pass in dummy frame to linker
    linker = helper.Linker(
        helper.convert_frame(df_1),
        {"link_type": "dedupe_only"},
        **helper.extra_linker_args(),
    )
    df_predict = linker.register_table(helper.convert_frame(df_e), "predict")
    df_clustered = linker.register_table(helper.convert_frame(df_c), "clusters")

    cm = linker._compute_graph_metrics(
        df_predict, df_clustered, threshold_match_probability=0.95
    )
    df_cm = cm.clusters.as_pandas_dataframe()

    expected = [
        {"cluster_id": 1, "n_nodes": 4, "n_edges": 4, "cluster_centralisation": 4 / 6},
        {"cluster_id": 2, "n_nodes": 6, "n_edges": 5, "cluster_centralisation": 8 / 20},
        {"cluster_id": 3, "n_nodes": 2, "n_edges": 1, "cluster_centralisation": None},
        {
            "cluster_id": 4,
            "n_nodes": 11,
            "n_edges": 19,
            "cluster_centralisation": 28 / 90,
        },
    ]
    for expected_row_details in expected:
        relevant_row = df_cm[df_cm["cluster_id"] == expected_row_details["cluster_id"]]
        assert relevant_row["n_nodes"].iloc[0] == expected_row_details["n_nodes"]
        assert relevant_row["n_edges"].iloc[0] == expected_row_details["n_edges"]
        # float to convert from Decimal
        density_computed = float(relevant_row["density"].iloc[0])
        density_expected = (
            2
            * expected_row_details["n_edges"]
            / (expected_row_details["n_nodes"] * (expected_row_details["n_nodes"] - 1))
        )
        assert density_computed == approx(density_expected)

        cc_computed = relevant_row["cluster_centralisation"].iloc[0]
        cc_expected = expected_row_details["cluster_centralisation"]
        # don't check None case as get inconsistent types from different backends
        if cc_expected is not None:
            assert float(cc_computed) == approx(
                expected_row_details["cluster_centralisation"]
            )

    df_nm = cm.nodes.as_pandas_dataframe()

    for unique_id, expected_node_degree in expected_node_degrees:
        relevant_row = df_nm[df_nm["composite_unique_id"] == unique_id]
        calculated_node_degree = relevant_row["node_degree"].iloc[0]
        assert calculated_node_degree == expected_node_degree, (
            f"Expected node degree {expected_node_degree} for node {unique_id}, "
            f"but found node degree {calculated_node_degree}"
        )
