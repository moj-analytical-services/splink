import pandas as pd
import sqlglot
from sqlglot.expressions import CTE, Identifier, TableAlias

from splink.duckdb.duckdb_comparison_library import (
    exact_match,
    levenshtein_at_thresholds,
)
from splink.duckdb.duckdb_linker import DuckDBLinker


def test_cache_tracking_works():

    data = [
        {"unique_id": 1, "name": "Amanda"},
        {"unique_id": 2, "name": "Robin"},
        {"unique_id": 3, "name": "Robyn"},
        {"unique_id": 4, "name": "David"},
        {"unique_id": 5, "name": "Eve"},
        {"unique_id": 6, "name": "Amanda"},
    ]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [levenshtein_at_thresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    linker = DuckDBLinker(df, settings)
    cache = linker._intermediate_table_cache

    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is False
    linker.estimate_u_using_random_sampling(target_rows=1e4)

    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is True

    linker.estimate_u_using_random_sampling(target_rows=1e4)
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is True
    )

    cache.reset_executed_queries_tracker()
    cache.reset_queries_retrieved_from_cache_tracker()
    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is False
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is False
    )
    linker.estimate_u_using_random_sampling(target_rows=1e4)
    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is False
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is True
    )

    linker.invalidate_cache()
    cache.reset_executed_queries_tracker()
    cache.reset_queries_retrieved_from_cache_tracker()
    linker.estimate_u_using_random_sampling(target_rows=1e4)
    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is True
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is False
    )


def test_cache_used_when_registering_nodes_table():
    df = [
        {"unique_id": 1, "name": "Amanda"},
        {"unique_id": 2, "name": "Robin"},
        {"unique_id": 3, "name": "Robyn"},
    ]
    df = pd.DataFrame(df)

    splink__df_concat_with_tf = [
        {"unique_id": 1, "name": "Amanda", "tf_name": 0.3},
        {"unique_id": 2, "name": "Robin", "tf_name": 0.2},
        {"unique_id": 3, "name": "Robyn", "tf_name": 0.5},
    ]
    splink__df_concat_with_tf = pd.DataFrame(splink__df_concat_with_tf)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            levenshtein_at_thresholds("name", 2, term_frequency_adjustments=True)
        ],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    linker = DuckDBLinker(df, settings)
    cache = linker._intermediate_table_cache
    linker.register_table_input_nodes_concat_with_tf(splink__df_concat_with_tf)
    linker.estimate_u_using_random_sampling(target_rows=1e4)
    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is False
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is True
    )


# This test is more tricky because tf tables are not explicitly retrieved from
# the cache if they exist because they are an intermediate part of a larger
# SQL query rather than being a table output in their own right
# Instead, if the cache is used, they SQL is simplified so that a CTE is
# no longer required to derive the table
def test_cache_used_when_registering_tf_tables():
    data = [
        {"unique_id": 1, "first_name": "Amanda", "surname": "Smith"},
        {"unique_id": 2, "first_name": "Robin", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Robyn", "surname": "Jones"},
    ]

    df = pd.DataFrame(data)

    surname_tf_table = pd.DataFrame(
        [
            {"surname": "Smith", "tf_surname": 0.3333333333333333},
            {"surname": "Jones", "tf_surname": 0.6666666666666666},
        ]
    )

    first_name_tf_table = pd.DataFrame(
        [
            {"first_name": "Amanda", "tf_first_name": 0.1},
            {"first_name": "Robin", "tf_first_name": 0.5},
            {"first_name": "Robyn", "tf_first_name": 0.4},
        ]
    )

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            exact_match("first_name", term_frequency_adjustments=True),
            exact_match("surname", term_frequency_adjustments=True),
        ],
        "blocking_rules_to_generate_predictions": ["l.surname = r.surname"],
    }

    # First test do not register any tf tables
    linker = DuckDBLinker(df, settings)
    cache = linker._intermediate_table_cache

    linker.estimate_u_using_random_sampling(target_rows=1e4)

    # Get table names generated using CTEs
    sql = cache["__splink__df_concat_with_tf"].sql_used_to_create
    tree = sqlglot.parse_one(sql)

    cte_table_aliases_used = []
    for cte in tree.find_all(CTE):
        ta = cte.find(TableAlias)
        table_alias_name = ta.find(Identifier).args["this"]
        cte_table_aliases_used.append(table_alias_name)

    # Both tf tables should have been calculated
    assert "__splink__df_tf_first_name" in cte_table_aliases_used
    assert "__splink__df_tf_surname" in cte_table_aliases_used

    # Then try the same after registering surname tf table
    linker = DuckDBLinker(df, settings)
    cache = linker._intermediate_table_cache
    linker.register_term_frequency_lookup(surname_tf_table, "surname")
    linker.estimate_u_using_random_sampling(target_rows=1e4)

    # Get table names generated using CTEs
    sql = cache["__splink__df_concat_with_tf"].sql_used_to_create
    tree = sqlglot.parse_one(sql)

    cte_table_aliases_used = []
    for cte in tree.find_all(CTE):
        ta = cte.find(TableAlias)
        table_alias_name = ta.find(Identifier).args["this"]
        cte_table_aliases_used.append(table_alias_name)

    assert "__splink__df_tf_first_name" in cte_table_aliases_used
    assert "__splink__df_tf_surname" not in cte_table_aliases_used

    # Then try the same after registering both
    linker = DuckDBLinker(df, settings)
    cache = linker._intermediate_table_cache
    linker.register_term_frequency_lookup(surname_tf_table, "surname")
    linker.register_term_frequency_lookup(first_name_tf_table, "first_name")
    linker.estimate_u_using_random_sampling(target_rows=1e4)

    # Get table names generated using CTEs
    sql = cache["__splink__df_concat_with_tf"].sql_used_to_create
    tree = sqlglot.parse_one(sql)

    cte_table_aliases_used = []
    for cte in tree.find_all(CTE):
        ta = cte.find(TableAlias)
        table_alias_name = ta.find(Identifier).args["this"]
        cte_table_aliases_used.append(table_alias_name)

    assert "__splink__df_tf_first_name" not in cte_table_aliases_used
    assert "__splink__df_tf_surname" not in cte_table_aliases_used
