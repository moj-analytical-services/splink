from unittest.mock import patch

from pytest import approx, raises

from splink.internals.comparison_library import ExactMatch
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker

from .decorator import mark_with_dialects_excluding

data_1 = [
    {"unique_id": 1, "first_name": "Tom", "surname": "Fox", "dob": "1980-01-01"},
    {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
    {"unique_id": 3, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
]

data_2 = [
    {"unique_id": 1, "first_name": "Bob", "surname": "Ray", "dob": "1999-09-22"},
    {"unique_id": 2, "first_name": "Amy", "surname": "Lee", "dob": "1980-01-01"},
]


def assert_approx_equal(data_real: list[dict], data_expected: list[dict]):
    """
    Compare two lists of dicts, using pytest.approx per field
    Bundle various reasonable exceptions together
    """
    # pytest.approx can't handle mixed types: https://github.com/pytest-dev/pytest/issues/13010
    # fix upcoming but too new
    assertion_exceptions: list[Exception] = []
    if len(data_real) != len(data_expected):
        assertion_exceptions.append(
            ValueError(
                f"Incompatible lengths: got {len(data_real)}, "
                f"expected {len(data_expected)}"
            )
        )
    for real, expected in zip(data_real, data_expected):
        if real.keys() != expected.keys():
            assertion_exceptions.append(
                KeyError(
                    f"Mismatched keys: got {real.keys()}, expected {expected.keys()}"
                )
            )
        for col in real:
            try:
                assert real[col] == approx(expected[col])
            except (AssertionError, KeyError) as e:
                assertion_exceptions.append(e)
    if assertion_exceptions:
        raise AssertionError(assertion_exceptions)


def test_size_density_dedupe():
    settings = {
        "probability_two_random_records_match": 0.01,
        "link_type": "dedupe_only",
        "comparisons": [
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
    }
    db_api = DuckDBAPI()
    df_1_sdf = db_api.register(data_1)

    linker = Linker(df_1_sdf, settings)

    df_predict = linker.inference.predict()
    df_clustered = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.9
    )

    # not testing cluster_centralisation here
    # it's not relevant for small clusters anyhow
    data_result = (
        linker.clustering.compute_graph_metrics(df_predict, df_clustered)
        .clusters.query_sql(
            """
            SELECT * EXCLUDE cluster_centralisation
            FROM {this}
            ORDER BY cluster_id
            """
        )
        .as_record_dict()
    )

    data_expected = [
        {"cluster_id": 1, "n_nodes": 1, "n_edges": 0.0, "density": None},
        {"cluster_id": 2, "n_nodes": 2, "n_edges": 1.0, "density": 1.0},
    ]

    assert data_result == data_expected


def test_size_density_link():
    settings = {
        "probability_two_random_records_match": 0.01,
        "link_type": "link_only",
        "comparisons": [
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
    }
    db_api = DuckDBAPI()
    df_1_sdf = db_api.register(data_1, dataset_display_name="df_left")
    df_2_sdf = db_api.register(data_2, dataset_display_name="df_right")

    linker = Linker(
        [df_1_sdf, df_2_sdf],
        settings,
    )

    df_predict = linker.inference.predict()
    df_clustered = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.9
    )

    data_result = (
        linker.clustering.compute_graph_metrics(
            df_predict, df_clustered, threshold_match_probability=0.99
        )
        .clusters.query_sql(
            """
            SELECT * EXCLUDE cluster_centralisation
            FROM {this}
            ORDER BY cluster_id
            """
        )
        .as_record_dict()
    )

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

    assert_approx_equal(data_result, data_expected)


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
    df_e = [
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
    df_c = (
        [{"cluster_id": 1, "unique_id": i} for i in range(1, 4 + 1)]
        + [{"cluster_id": 2, "unique_id": i} for i in range(5, 10 + 1)]
        + [{"cluster_id": 3, "unique_id": i} for i in range(11, 12 + 1)]
        + [{"cluster_id": 4, "unique_id": i} for i in range(13, 23 + 1)]
        + [{"cluster_id": 5, "unique_id": i} for i in range(24, 24 + 1)]
    )

    expected_node_metrics = [
        # cluster 1
        # max degree 3
        # centralisation = (1 + 2 + 1)/(3 * 2)
        (1, 3, 1.0),
        (2, 2, 2.0 / 3),
        (3, 1, 1.0 / 3),
        (4, 2, 2.0 / 3),
        # cluster 2
        # centralisation = (2 + 1 + 2 + 1 + 2)/(5 * 4)
        (5, 3, 0.6),
        (6, 1, 0.2),
        (7, 2, 0.4),
        (8, 1, 0.2),
        (9, 2, 0.4),
        (10, 1, 0.2),
        # cluster 3
        # centralisation = NULL
        (11, 1, 1.0),
        (12, 1, 1.0),
        # cluster 4
        # centralisation = (3 + 2 + 1 + 3 + 1 + 4 + 3 + 4 + 3 + 4)/(10*9)
        (13, 6, 0.6),
        (14, 3, 0.3),
        (15, 4, 0.4),
        (16, 5, 0.5),
        (17, 3, 0.3),
        (18, 5, 0.5),
        (19, 2, 0.2),
        (20, 3, 0.3),
        (21, 2, 0.2),
        (22, 3, 0.3),
        (23, 2, 0.2),
        # cluster 5
        # centralisation = NULL
        (24, 0, 0.0),
    ]

    # pass in dummy frame to linker
    linker = helper.linker_with_registration(
        [data_1],
        {"link_type": "dedupe_only"},
    )
    df_predict = linker.table_management.register_table(df_e, "predict")
    df_clustered = linker.table_management.register_table(df_c, "clusters")

    cm = linker.clustering.compute_graph_metrics(
        df_predict, df_clustered, threshold_match_probability=0.95
    )
    df_cm = cm.clusters.as_record_dict()

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
        relevant_row = list(
            filter(
                lambda row: row["cluster_id"] == expected_row_details["cluster_id"],
                df_cm,
            )
        )[0]
        assert relevant_row["n_nodes"] == expected_row_details["n_nodes"]
        assert relevant_row["n_edges"] == expected_row_details["n_edges"]
        # float to convert from Decimal
        density_computed = float(relevant_row["density"])
        density_expected = (
            2
            * expected_row_details["n_edges"]
            / (expected_row_details["n_nodes"] * (expected_row_details["n_nodes"] - 1))
        )
        assert density_computed == approx(density_expected)

        cc_computed = relevant_row["cluster_centralisation"]
        cc_expected = expected_row_details["cluster_centralisation"]
        # don't check None case as get inconsistent types from different backends
        if cc_expected is not None:
            assert float(cc_computed) == approx(
                expected_row_details["cluster_centralisation"]
            )

    df_nm = cm.nodes.as_record_dict()

    for unique_id, expected_degree, expected_centrality in expected_node_metrics:
        relevant_row = list(
            filter(lambda row: row["composite_unique_id"] == unique_id, df_nm)
        )[0]
        calculated_node_degree = relevant_row["node_degree"]
        assert calculated_node_degree == expected_degree, (
            f"Expected node degree {expected_degree} for node {unique_id}, "
            f"but found node degree {calculated_node_degree}"
        )
        calculated_node_centrality = relevant_row["node_centrality"]
        assert float(calculated_node_centrality) == approx(expected_centrality), (
            f"Expected node centrality {expected_centrality} for node {unique_id}, "
            f"but found node centrality {calculated_node_centrality}"
        )


def make_edge_row(
    id_l: int, id_r: int, group_id: int, match_probability: float, is_bridge: bool
):
    return {
        "unique_id_l": id_l,
        "unique_id_r": id_r,
        "cluster_id": group_id,
        "match_probability": match_probability,
        "is_bridge": is_bridge,
    }


@mark_with_dialects_excluding()
def test_is_bridge(dialect, test_helpers):
    helper = test_helpers[dialect]
    df_e = [
        # cluster 1 - triangle with offshoot
        # 4 nodes, 4 edges
        make_edge_row(1, 2, 1, 0.96, True),
        make_edge_row(2, 3, 1, 0.96, False),
        make_edge_row(3, 4, 1, 0.96, False),
        make_edge_row(2, 4, 1, 0.96, False),
        # cluster 2 - 2 triangles joined by bridge
        # 6 nodes, 7 edges
        make_edge_row(5, 6, 2, 0.95, False),
        make_edge_row(6, 7, 2, 0.96, False),
        make_edge_row(7, 5, 2, 0.99, False),
        make_edge_row(8, 9, 2, 0.96, False),
        make_edge_row(9, 10, 2, 0.96, False),
        make_edge_row(10, 8, 2, 0.96, False),
        make_edge_row(5, 10, 2, 0.96, True),
        # cluster 2 - 2 triangles joined by bridge
        # 7 nodes, 9 edges
        make_edge_row(11, 12, 3, 0.96, False),
        make_edge_row(11, 13, 3, 0.96, False),
        make_edge_row(13, 14, 3, 0.96, False),
        make_edge_row(12, 14, 3, 0.96, False),
        make_edge_row(12, 15, 3, 0.96, False),
        make_edge_row(13, 18, 3, 0.96, False),
        make_edge_row(15, 18, 3, 0.96, False),
        make_edge_row(16, 17, 3, 0.96, True),
        make_edge_row(17, 18, 3, 0.96, True),
        # not 'real' edges, shouldn't break things:
        make_edge_row(1, 3, 1, 0.92, None),
        make_edge_row(1, 6, 2, 0.945, None),
        make_edge_row(5, 9, 2, 0.9, None),
        make_edge_row(1, 13, 3, 0.9, None),
        make_edge_row(6, 16, 3, 0.9, None),
    ]

    df_c = (
        [{"cluster_id": 1, "unique_id": i} for i in range(1, 4 + 1)]
        + [{"cluster_id": 2, "unique_id": i} for i in range(5, 10 + 1)]
        + [{"cluster_id": 3, "unique_id": i} for i in range(11, 18 + 1)]
    )
    linker = helper.linker_with_registration(
        [data_1],
        {"link_type": "dedupe_only"},
    )
    df_predict = linker.table_management.register_table(df_e, "br_predict")
    df_clustered = linker.table_management.register_table(df_c, "br_clusters")

    # linker.debug_mode = True
    cm = linker.clustering.compute_graph_metrics(
        df_predict, df_clustered, threshold_match_probability=0.95
    )
    df_em = cm.edges.as_record_dict()

    for row in df_e:
        node_l, node_r = (
            row["unique_id_l"],
            row["unique_id_r"],
        )
        relevant_row = list(
            filter(
                lambda row: (
                    row["composite_unique_id_l"] == node_l
                    and row["composite_unique_id_r"] == node_r
                ),
                df_em,
            )
        )
        expected_is_bridge = row["is_bridge"]
        if expected_is_bridge is None:
            assert not relevant_row
        else:
            calculated_is_bridge = relevant_row[0]["is_bridge"]
            assert calculated_is_bridge == expected_is_bridge, (
                f"Expected is_bridge {expected_is_bridge} for edge {node_l}, {node_r}, "
                f"but found is_bridge: {calculated_is_bridge}"
            )


unpatched_import = __import__


def mock_no_igraph_installed(name, *args):
    if name == "igraph":
        raise ModuleNotFoundError("Mocking missing 'igraph' in test")
    return unpatched_import(name, *args)


def test_edges_without_igraph():
    settings = {
        "probability_two_random_records_match": 0.01,
        "link_type": "dedupe_only",
        "comparisons": [
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
    }
    db_api = DuckDBAPI()
    df_1_sdf = db_api.register(data_1)
    linker = Linker(df_1_sdf, settings)

    df_predict = linker.inference.predict()
    df_clustered = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.9
    )

    # pretend we don't have igraph installed
    with patch("builtins.__import__", side_effect=mock_no_igraph_installed):
        graph_metrics = linker.clustering.compute_graph_metrics(
            df_predict, df_clustered, threshold_match_probability=0.9
        )
    df_edge_metrics = graph_metrics.edges.as_dict()
    assert "composite_unique_id_l" in df_edge_metrics
    assert "composite_unique_id_r" in df_edge_metrics
    assert "is_bridge" not in df_edge_metrics


def test_no_threshold_provided():
    df_e = [
        {
            "unique_id_l": 1,
            "name_l": "trame",
            "unique_id_r": 2,
            "name_r": "scrame",
            "match_probability": 0.99,
        },
    ]

    df_c = [
        {"cluster_id": 1, "unique_id": 1, "name": "trame"},
        {"cluster_id": 1, "unique_id": 2, "name": "scrame"},
    ]

    settings = {"link_type": "dedupe_only"}
    db_api = DuckDBAPI()
    df_1_sdf = db_api.register(data_1)
    linker = Linker(df_1_sdf, settings)

    df_predict = linker.table_management.register_table(df_e, "predict")
    df_clustered = linker.table_management.register_table(df_c, "clusters")

    with raises(TypeError):
        # no threshold_match_probability, no metadata
        _ = linker.clustering.compute_graph_metrics(df_predict, df_clustered)


def test_override_metadata_threshold():
    df_e = [
        # three edges at >= 0.9
        # two at >= 0.95
        make_edge_row(1, 2, 1, 0.95, None),
        make_edge_row(2, 3, 1, 0.96, None),
        make_edge_row(1, 3, 1, 0.92, None),
    ]
    df_c = [{"cluster_id": 1, "unique_id": i} for i in range(1, 3 + 1)]
    settings = {"link_type": "dedupe_only"}
    db_api = DuckDBAPI()
    df_1_sdf = db_api.register(data_1)
    linker = Linker(df_1_sdf, settings)
    # linker.debug_mode = True
    df_predict = linker.table_management.register_table(df_e, "predict")
    df_clustered = linker.table_management.register_table(df_c, "clusters")
    df_clustered.metadata["threshold_match_probability"] = 0.95

    gm_results_95 = linker.clustering.compute_graph_metrics(df_predict, df_clustered)
    gm_results_9 = linker.clustering.compute_graph_metrics(
        df_predict, df_clustered, threshold_match_probability=0.9
    )
    df_expected_95 = [
        {
            "cluster_id": 1,
            "n_nodes": 3,
            "n_edges": 2.0,
            "density": 2 / 3,
            "cluster_centralisation": 1.0,
        },
    ]

    df_expected_9 = [
        {
            "cluster_id": 1,
            "n_nodes": 3,
            "n_edges": 3.0,
            "density": 1.0,
            "cluster_centralisation": 0.0,
        },
    ]

    assert_approx_equal(
        gm_results_95.clusters.as_record_dict(),
        df_expected_95,
    )
    assert_approx_equal(
        gm_results_9.clusters.as_record_dict(),
        df_expected_9,
    )
