import pandas as pd

from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl

def test_clear_error_when_empty_block():
    
    data = [
        {"unique_id": 1, "name": "Amanda", "surname": "Smith"},
        {"unique_id": 2, "name": "Robin", "surname": "Jones"},
        {"unique_id": 3, "name": "Robyn", "surname": "Williams"},
        {"unique_id": 4, "name": "David", "surname": "Green"},
        {"unique_id": 5, "name": "Eve", "surname": "Pope"},
        {"unique_id": 6, "name": "Amanda", "surname": "Anderson"},
    ]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.levenshtein_at_thresholds("name", 1), cl.exact_match("surname")],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = True
    linker.estimate_u_using_random_sampling(target_rows=1e6)
    linker.estimate_parameters_using_expectation_maximisation("l.name = r.name")
    # this raises an error as block is empty, but should be a nicer error!
    linker.estimate_parameters_using_expectation_maximisation("l.surname = r.surname")
