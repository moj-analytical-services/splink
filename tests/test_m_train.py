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

    db_api.register(labels, "labels")
    linker_pairwise.training.estimate_m_from_pairwise_labels("labels")
    cc_name = linker_pairwise._settings_obj.comparisons[0]

    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)
    assert cl_exact.m_probability == 1 / 4
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.m_probability == 2 / 4
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.m_probability == 1 / 4
