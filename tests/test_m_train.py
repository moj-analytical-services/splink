import pandas as pd

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
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [LevenshteinAtThresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    # Train from label column
    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    linker.training.estimate_m_from_label_column("cluster")
    cc_name = linker._settings_obj.comparisons[0]

    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)
    assert cl_exact.m_probability == 1 / 4
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.m_probability == 2 / 4
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.m_probability == 1 / 4

    # Train from pairwise labels
    df["source_dataset"] = "fake_data_1"
    df_l = df[["unique_id", "source_dataset", "cluster"]].copy()
    df_r = df_l.copy()

    df_labels = df_l.merge(df_r, on="cluster", suffixes=("_l", "_r"))
    f1 = df_labels["unique_id_l"] < df_labels["unique_id_r"]
    df_labels = df_labels[f1].copy()

    for r in df_labels.iterrows():
        val = r[1]
        uid_l = val["unique_id_l"]
        uid_r = val["unique_id_r"]
        if val["cluster"] == 2:
            val["unique_id_l"] = uid_r
            val["unique_id_r"] = uid_l

    db_api = DuckDBAPI()

    linker_pairwise = Linker(df, settings, db_api=db_api)

    linker_pairwise.table_management.register_table(df_labels, "labels")
    linker_pairwise.training.estimate_m_from_pairwise_labels("labels")
    cc_name = linker_pairwise._settings_obj.comparisons[0]

    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)
    assert cl_exact.m_probability == 1 / 4
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.m_probability == 2 / 4
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.m_probability == 1 / 4
