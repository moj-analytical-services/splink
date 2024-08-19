import duckdb
import pandas as pd

from splink.internals.comparison_library import ExactMatch, LevenshteinAtThresholds
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker


def get_duckdb_table_names_as_list(con):
    result = con.execute("SHOW TABLES")
    tables = result.fetchall()
    return [x[0] for x in tables]


def test_cache_tracking_works():
    data = [
        {"unique_id": 1, "name": "Amanda"},
        {"unique_id": 2, "name": "Robin"},
        {"unique_id": 3, "name": "Robyn"},
    ]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [ExactMatch("name").configure(term_frequency_adjustments=True)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    cache = linker._intermediate_table_cache

    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is False
    linker.inference.predict()

    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is True

    linker.inference.predict()
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is True
    )

    cache.reset_executed_queries_tracker()
    cache.reset_queries_retrieved_from_cache_tracker()
    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is False

    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is False
    )
    linker.inference.predict()
    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is False
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is True
    )

    linker.table_management.invalidate_cache()
    cache.reset_executed_queries_tracker()
    cache.reset_queries_retrieved_from_cache_tracker()
    linker.inference.predict()
    # Triggers adding to queries retrieved from cache
    linker.inference.predict()
    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is True
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is True
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
            LevenshteinAtThresholds("name", 2).configure(
                term_frequency_adjustments=True
            )
        ],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    cache = linker._intermediate_table_cache
    linker.table_management.register_table_input_nodes_concat_with_tf(
        splink__df_concat_with_tf
    )
    linker.inference.predict()
    assert cache.is_in_executed_queries("__splink__df_concat_with_tf") is False
    assert (
        cache.is_in_queries_retrieved_from_cache("__splink__df_concat_with_tf") is True
    )


# This test is more tricky because term freq tables are not explicitly retrieved from
# the cache. When they exist they are an intermediate part of a larger
# SQL query rather than being tables which are outputted in their own right
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
            ExactMatch("first_name").configure(term_frequency_adjustments=True),
            ExactMatch("surname").configure(term_frequency_adjustments=True),
        ],
        "blocking_rules_to_generate_predictions": ["l.surname = r.surname"],
    }

    # First test do not register any tf tables
    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    cache = linker._intermediate_table_cache

    linker.inference.predict()

    assert not cache.is_in_queries_retrieved_from_cache("__splink__df_tf_first_name")
    assert not cache.is_in_queries_retrieved_from_cache("__splink__df_tf_surname")

    # Then try the same after registering surname tf table
    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    cache = linker._intermediate_table_cache
    linker.table_management.register_term_frequency_lookup(surname_tf_table, "surname")
    linker.inference.predict()

    assert not cache.is_in_queries_retrieved_from_cache("__splink__df_tf_first_name")
    assert cache.is_in_queries_retrieved_from_cache("__splink__df_tf_surname")

    # Then try the same after registering both
    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    cache = linker._intermediate_table_cache
    linker.table_management.register_term_frequency_lookup(surname_tf_table, "surname")
    linker.table_management.register_term_frequency_lookup(
        first_name_tf_table, "first_name"
    )
    linker.inference.predict()

    assert cache.is_in_queries_retrieved_from_cache("__splink__df_tf_first_name")
    assert cache.is_in_queries_retrieved_from_cache("__splink__df_tf_surname")


def test_cache_invalidation():
    data = [
        {"unique_id": 1, "name": "Amanda"},
        {"unique_id": 2, "name": "Robin"},
        {"unique_id": 3, "name": "Robyn"},
    ]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [LevenshteinAtThresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    cache = linker._intermediate_table_cache

    linker.table_management.compute_tf_table("name")
    len_before = len(cache.executed_queries)
    linker.table_management.compute_tf_table("name")
    len_after = len(cache.executed_queries)

    # If cache not invalidated, cache should be used
    assert len_before == len_after
    assert cache.is_in_queries_retrieved_from_cache("__splink__df_tf_name")

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    cache = linker._intermediate_table_cache

    linker.table_management.compute_tf_table("name")
    len_before = len(cache.executed_queries)
    linker.table_management.invalidate_cache()
    linker.table_management.compute_tf_table("name")
    len_after = len(cache.executed_queries)
    # If cache is invalidated, an additional query should have been executed
    assert len_before + 1 == len_after
    assert not cache.is_in_queries_retrieved_from_cache("__splink__df_tf_name")


def test_table_deletions():
    con = duckdb.connect()
    df = pd.DataFrame(  # noqa: F841
        [
            {"unique_id": 1, "name": "Amanda"},
            {"unique_id": 2, "name": "Robin"},
            {"unique_id": 3, "name": "Robyn"},
        ]
    )

    con.execute("CREATE TABLE my_table AS SELECT * FROM df")

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [LevenshteinAtThresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    db_api = DuckDBAPI(connection=con)

    linker = Linker("my_table", settings, db_api=db_api)

    table_names_before = set(get_duckdb_table_names_as_list(db_api._con))

    linker.table_management.compute_tf_table("name")
    linker.training.estimate_u_using_random_sampling(max_pairs=1e4)

    # # The database should be empty except for the original non-splink table
    linker.table_management.delete_tables_created_by_splink_from_db()

    table_names_after = set(get_duckdb_table_names_as_list(db_api._con))
    assert table_names_before == table_names_after
    assert table_names_after == set(["my_table"])


def test_table_deletions_with_preregistered():
    con = duckdb.connect()
    df = [
        {"unique_id": 1, "name": "Amanda"},
        {"unique_id": 2, "name": "Robin"},
        {"unique_id": 3, "name": "Robyn"},
    ]
    df = pd.DataFrame(df)
    con.execute("CREATE TABLE my_data_table AS SELECT * FROM df")

    splink__df_concat_with_tf = [
        {"unique_id": 1, "name": "Amanda", "tf_name": 0.3},
        {"unique_id": 2, "name": "Robin", "tf_name": 0.2},
        {"unique_id": 3, "name": "Robyn", "tf_name": 0.5},
    ]
    splink__df_concat_with_tf = pd.DataFrame(splink__df_concat_with_tf)
    con.execute(
        """
        CREATE TABLE my_nodes_with_tf_table
        AS SELECT * FROM splink__df_concat_with_tf
        """
    )

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            LevenshteinAtThresholds("name", 2).configure(
                term_frequency_adjustments=True
            )
        ],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    db_api = DuckDBAPI(connection=con)

    linker = Linker("my_data_table", settings, db_api=db_api)
    linker.table_management.register_table_input_nodes_concat_with_tf(
        "my_nodes_with_tf_table"
    )

    table_names_before = set(get_duckdb_table_names_as_list(db_api._con))

    linker.table_management.compute_tf_table("name")
    linker.training.estimate_u_using_random_sampling(max_pairs=1e4)
    # Note we shouldn't have executed a __splink__df_concat_with_tf query

    cache = linker._intermediate_table_cache
    assert not cache.is_in_executed_queries("__splink__df_concat_with_tf")

    linker.table_management.delete_tables_created_by_splink_from_db()

    table_names_after = set(get_duckdb_table_names_as_list(db_api._con))

    assert table_names_before == table_names_after


def test_single_deletion():
    data = [
        {"unique_id": 1, "name": "Amanda"},
        {"unique_id": 2, "name": "Robin"},
        {"unique_id": 3, "name": "Robyn"},
    ]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [LevenshteinAtThresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    cache = linker._intermediate_table_cache

    tf_table = linker.table_management.compute_tf_table("name")
    table_name = tf_table.physical_name
    # Check it is in the cache and database
    assert table_name in get_duckdb_table_names_as_list(db_api._con)
    assert "__splink__df_tf_name" in cache

    tf_table.drop_table_from_database_and_remove_from_cache()
    # Check it is no longer in the cache or database
    assert table_name not in get_duckdb_table_names_as_list(db_api._con)
    assert "__splink__df_tf_name" not in cache
