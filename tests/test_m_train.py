from splink.internals.comparison_library import LevenshteinAtThresholds
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker


def test_m_train():
    data = [
        {"unique_id": 1, "name": "Robin", "cluster": 1},
        {"unique_id": 2, "name": "Robyn", "cluster": 1},
        {"unique_id": 3, "name": "Robin", "cluster": 1},
        {"unique_id": 4, "name": "James", "cluster": 2},
        {"unique_id": 5, "name": "David", "cluster": 2},
    ]

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [LevenshteinAtThresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    # Train from label column
    db_api = DuckDBAPI()

    df_sdf = db_api.register(data)
    linker = Linker(df_sdf, settings)

    linker.training.estimate_m_from_label_column("cluster")
    cc_name = linker._settings_obj.comparisons[0]

    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)
    assert cl_exact.m_probability == 1 / 4
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.m_probability == 2 / 4
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.m_probability == 1 / 4

    # Train from pairwise labels
    labels = [
        {
            "cluster": row_l["cluster"],  # equal so just pick one
            "source_dataset_l": "fake_data_1",
            "source_dataset_r": "fake_data_1",
            "unique_id_l": row_l["unique_id"],
            "unique_id_r": row_r["unique_id"],
        }
        for row_l in data
        for row_r in data
        if row_l["cluster"] == row_r["cluster"]
        and row_l["unique_id"] < row_r["unique_id"]
    ]

    db_api = DuckDBAPI()

    df_sdf = db_api.register(data)
    linker_pairwise = Linker(df_sdf, settings)

    db_api.register(labels, table_name="labels")
    linker_pairwise.training.estimate_m_from_pairwise_labels("labels")
    cc_name = linker_pairwise._settings_obj.comparisons[0]

    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)
    assert cl_exact.m_probability == 1 / 4
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.m_probability == 2 / 4
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.m_probability == 1 / 4


def _fixed_match_weight_name_settings():
    return {
        "link_type": "dedupe_only",
        "comparisons": [
            {
                "output_column_name": "name",
                "comparison_levels": [
                    {
                        "sql_condition": '"name_l" = "name_r"',
                        "label_for_charts": "Exact match",
                        "fixed_match_weight": 2,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                    },
                ],
            }
        ],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }


def test_m_train_label_column_respects_fixed_match_weight():
    data = [
        {"unique_id": 1, "name": "Robin", "cluster": 1},
        {"unique_id": 2, "name": "Robyn", "cluster": 1},
        {"unique_id": 3, "name": "Robin", "cluster": 1},
        {"unique_id": 4, "name": "James", "cluster": 2},
        {"unique_id": 5, "name": "David", "cluster": 2},
    ]

    db_api = DuckDBAPI()
    df_sdf = db_api.register(data)
    linker = Linker(df_sdf, _fixed_match_weight_name_settings())

    linker.training.estimate_m_from_label_column("cluster")

    exact_level = linker._settings_obj.comparisons[
        0
    ]._get_comparison_level_by_comparison_vector_value(1)
    # The fixed match weight must be unchanged by training
    assert exact_level.fixed_match_weight == 2
    assert exact_level.m_probability == 1.0
    assert exact_level.u_probability == 2**-2


def test_m_train_pairwise_labels_respects_fixed_match_weight():
    data = [
        {"unique_id": 1, "name": "Robin", "cluster": 1},
        {"unique_id": 2, "name": "Robyn", "cluster": 1},
        {"unique_id": 3, "name": "Robin", "cluster": 1},
        {"unique_id": 4, "name": "James", "cluster": 2},
        {"unique_id": 5, "name": "David", "cluster": 2},
    ]
    labels = [
        {
            "cluster": row_l["cluster"],
            "source_dataset_l": "fake_data_1",
            "source_dataset_r": "fake_data_1",
            "unique_id_l": row_l["unique_id"],
            "unique_id_r": row_r["unique_id"],
        }
        for row_l in data
        for row_r in data
        if row_l["cluster"] == row_r["cluster"]
        and row_l["unique_id"] < row_r["unique_id"]
    ]

    db_api = DuckDBAPI()
    df_sdf = db_api.register(data)
    linker = Linker(df_sdf, _fixed_match_weight_name_settings())
    db_api.register(labels, table_name="labels")

    linker.training.estimate_m_from_pairwise_labels("labels")

    exact_level = linker._settings_obj.comparisons[
        0
    ]._get_comparison_level_by_comparison_vector_value(1)
    assert exact_level.fixed_match_weight == 2
    assert exact_level.m_probability == 1.0
    assert exact_level.u_probability == 2**-2
