from splink.comparison_library import levenshtein
from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd


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
        "comparisons": [levenshtein("name")],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    linker = DuckDBLinker(settings, input_tables={"fake_data_1": df})
    linker.debug_mode = True
    linker.train_m_from_label_column("cluster")
    cc_name = linker.settings_obj.comparisons[0]

    cl_exact = cc_name.get_comparison_level_by_comparison_vector_value(2)
    assert cl_exact.m_probability == 1 / 4
    cl_lev = cc_name.get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.m_probability == 2 / 4
    cl_no = cc_name.get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.m_probability == 1 / 4
    assert linker.settings_obj._blocking_rules_to_generate_predictions == [
        "l.name = r.name"
    ]
