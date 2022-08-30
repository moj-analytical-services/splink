from splink.duckdb.duckdb_comparison_library import levenshtein_at_thresholds
from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd
from splink.comparison_level_library import _mutable_params


def test_u_train():

    data = [
        {"unique_id": 1, "name": "Amanda"},
        {"unique_id": 2, "name": "Robin"},
        {"unique_id": 3, "name": "Robyn"},
        {"unique_id": 4, "name": "David"},
        {"unique_id": 5, "name": "Eve"},
        {"unique_id": 6, "name": "Amanda"},
    ]
    df = pd.DataFrame(data)

    _mutable_params["dialect"] = "duckdb"
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [levenshtein_at_thresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = True
    linker.estimate_u_using_random_sampling(target_rows=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    denom = (6 * 5) / 2  # n(n-1) / 2
    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)
    assert cl_exact.u_probability == 1 / denom
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.u_probability == 1 / denom
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.u_probability == (denom - 2) / denom

    br = linker._settings_obj._blocking_rules_to_generate_predictions[0]
    assert br.blocking_rule == "l.name = r.name"
