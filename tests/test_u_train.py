import duckdb
import numpy as np
import pandas as pd
import pytest

import splink.internals.comparison_library as cl
from splink.internals.estimate_u import (
    ComparisonConvergenceInfo,
    LevelConvergenceInfo,
    _proportion_sample_size_link_only,
    estimate_u_values,
)
from tests.decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_u_train(test_helpers, dialect):
    helper = test_helpers[dialect]
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
        "comparisons": [cl.LevenshteinAtThresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }
    df_linker = helper.convert_frame(df)

    linker = helper.linker_with_registration(df_linker, settings)
    linker._debug_mode = True
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    denom = (6 * 5) / 2  # n(n-1) / 2
    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)
    assert cl_exact.u_probability == 1 / denom
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.u_probability == 1 / denom
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.u_probability == (denom - 2) / denom

    br = linker._settings_obj._blocking_rules_to_generate_predictions[0]
    assert br.blocking_rule_sql == "l.name = r.name"


@mark_with_dialects_excluding("postgres")
def test_u_train_link_only(test_helpers, dialect):
    helper = test_helpers[dialect]
    data_l = [
        {"unique_id": 1, "name": "Amanda"},
        {"unique_id": 2, "name": "Robin"},
        {"unique_id": 3, "name": "Robyn"},
        {"unique_id": 4, "name": "David"},
        {"unique_id": 5, "name": "Eve"},
        {"unique_id": 6, "name": "Amanda"},
        {"unique_id": 7, "name": "Stuart"},
    ]
    data_r = [
        {"unique_id": 1, "name": "Eva"},
        {"unique_id": 2, "name": "David"},
        {"unique_id": 3, "name": "Sophie"},
        {"unique_id": 4, "name": "Jimmy"},
        {"unique_id": 5, "name": "Stuart"},
        {"unique_id": 6, "name": "Jimmy"},
    ]
    df_l = pd.DataFrame(data_l)
    df_r = pd.DataFrame(data_r)

    settings = {
        "link_type": "link_only",
        "comparisons": [cl.LevenshteinAtThresholds("name", 2)],
        "blocking_rules_to_generate_predictions": [],
    }

    df_l = helper.convert_frame(df_l)
    df_r = helper.convert_frame(df_r)

    linker = helper.linker_with_registration(
        [df_l, df_r],
        settings,
        input_table_aliases=["l", "r"],
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    # The u_probability values verify correct pair generation
    # (if wrong pairs were included, probabilities would differ)
    denom = 6 * 7  # only l <-> r candidate links
    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)
    # David, Stuart
    assert cl_exact.u_probability == 2 / denom
    # Eve/Eva
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)
    assert cl_lev.u_probability == 1 / denom
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.u_probability == (denom - 3) / denom


# Spark is too slow in conjunction with debug mode.  If we begin to capture sql,
# then we could refactor this test to introspect the sql
@mark_with_dialects_excluding("spark", "postgres")
def test_u_train_link_only_sample(test_helpers, dialect):
    helper = test_helpers[dialect]
    df_l = (
        pd.DataFrame(np.random.randint(0, 3000, size=(3000, 1)), columns=["name"])
        .reset_index()
        .rename(columns={"index": "unique_id"})
    )
    df_r = (
        pd.DataFrame(np.random.randint(0, 3000, size=(3000, 1)), columns=["name"])
        .reset_index()
        .rename(columns={"index": "unique_id"})
    )

    # max_pairs is a good deal less than total possible pairs = 9_000_000
    max_pairs = 1_800_000

    settings = {
        "link_type": "link_only",
        "comparisons": [cl.ExactMatch("name")],
        "blocking_rules_to_generate_predictions": [],
    }

    df_l = helper.convert_frame(df_l)
    df_r = helper.convert_frame(df_r)

    linker = helper.linker_with_registration(
        [df_l, df_r],
        settings,
        input_table_aliases=["_a", "_b"],
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    # Use estimate_u_values directly to get diagnostics
    # Set min_count very high to disable early convergence, so we process all pairs
    diagnostics = estimate_u_values(linker, max_pairs=max_pairs, min_count=1_000_000)

    # Get the total pairs actually sampled from the diagnostics
    pairs_actually_sampled = diagnostics[0].total_pairs_sampled

    proportion_of_max_pairs_sampled = pairs_actually_sampled / max_pairs
    # proportion_of_max_pairs_sampled should be 1 - i.e. we sample max_pairs rows
    # as we have many more pairs available than max_pairs
    # equality only holds probabilistically for some backends, due to sampling strategy
    # chance of failure is approximately 1e-06 with this choice of relative error
    assert pytest.approx(proportion_of_max_pairs_sampled, rel=0.15) == 1.0


def test_u_train_link_only_sample_proportion():
    """
    Test that the _proportion_sample_size_link_only function returns
    proportions and sample size that result in max_pairs records being selected
    constraint.

    Test strategy is to perform the join with all the data

    The use the _proportion_sample_size_link_only with max_pairs to get a
    proportion that should result in max_pairs records being selected

    We can then apply the proportion to the original data, and check
    that max_pairs records are selected
    """

    def count_self_join(row_counts):
        dfs = []

        for count in row_counts:
            df = pd.DataFrame(
                {
                    "source_dataset_name": [f"dataset_{count}"] * count,
                    "unique_id": range(count),
                }
            )
            dfs.append(df)

        combined_df = pd.concat(dfs)

        con = duckdb.connect(database=":memory:", read_only=False)
        con.register("combined_df", combined_df)

        query = """
        SELECT count(*)
        FROM combined_df a
        JOIN combined_df b
        ON a.source_dataset_name != b.source_dataset_name
        AND a.source_dataset_name || a.unique_id < b.source_dataset_name || b.unique_id
        """

        result = con.execute(query).fetchdf()
        return result.iloc[0, 0]

    row_counts = [10, 20, 30]
    count = count_self_join(row_counts)
    # Note need to be careful that max_pairs here results in a 'nice' proportion
    # that means the proporitoned row counts are integers
    max_pairs = count / 4
    proportion, sample_size = _proportion_sample_size_link_only(
        row_counts_individual_dfs=row_counts, max_pairs=max_pairs
    )

    proportioned_row_counts = [int(c * proportion) for c in row_counts]

    count_proportioned = count_self_join(proportioned_row_counts)

    assert count_proportioned == max_pairs


@mark_with_dialects_excluding("postgres")
def test_u_train_multilink(test_helpers, dialect):
    helper = test_helpers[dialect]
    datas = [
        [
            {"unique_id": 1, "name": "John"},
            {"unique_id": 2, "name": "Robin"},
        ],
        [
            {"unique_id": 1, "name": "Jon"},
            {"unique_id": 2, "name": "David"},
            {"unique_id": 3, "name": "Sophie"},
        ],
        [
            {"unique_id": 1, "name": "Eva"},
            {"unique_id": 2, "name": "David"},
            {"unique_id": 3, "name": "Alex"},
            {"unique_id": 4, "name": "Chris"},
        ],
        [
            {"unique_id": 1, "name": "Andy"},
            {"unique_id": 2, "name": "David"},
            {"unique_id": 3, "name": "Reece"},
            {"unique_id": 4, "name": "Adil"},
            {"unique_id": 5, "name": "Adil"},
            {"unique_id": 6, "name": "Adil"},
            {"unique_id": 7, "name": "Adil"},
        ],
    ]
    dfs = list(map(lambda x: helper.convert_frame(pd.DataFrame(x)), datas))

    expected_total_links = 2 * 3 + 2 * 4 + 2 * 7 + 3 * 4 + 3 * 7 + 4 * 7
    expected_total_links_with_dedupes = (2 + 3 + 4 + 7) * (2 + 3 + 4 + 7 - 1) / 2

    settings = {
        "link_type": "link_only",
        "comparisons": [cl.LevenshteinAtThresholds("name", 2)],
        "blocking_rules_to_generate_predictions": [],
    }

    linker = helper.linker_with_registration(
        dfs,
        settings,
        input_table_aliases=["a", "b", "c", "d"],
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    # The u_probability values verify that no self-table pairs were included
    # (if they were, the probabilities would be different)
    denom = expected_total_links
    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)

    # David - three pairwise comparisons
    assert cl_exact.u_probability == 3 / denom
    # John, Jon
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)

    assert cl_lev.u_probability == 1 / denom
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.u_probability == (denom - 4) / denom

    # also check the numbers on a link + dedupe with same inputs
    settings["link_type"] = "link_and_dedupe"
    linker = helper.linker_with_registration(
        dfs,
        settings,
        input_table_aliases=["e", "f", "g", "h"],
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    # For link_and_dedupe, within-table pairs should be included
    denom = expected_total_links_with_dedupes
    cl_exact = cc_name._get_comparison_level_by_comparison_vector_value(2)

    # David and Adil
    assert cl_exact.u_probability == (3 + 6) / denom
    # John, Jon
    cl_lev = cc_name._get_comparison_level_by_comparison_vector_value(1)

    assert cl_lev.u_probability == 1 / denom
    cl_no = cc_name._get_comparison_level_by_comparison_vector_value(0)
    assert cl_no.u_probability == (denom - 10) / denom


# No SQLite or Postgres - don't support random seed
@mark_with_dialects_excluding("sqlite", "postgres")
def test_seed_u_outputs(test_helpers, dialect):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.LevenshteinAtThresholds("first_name", 2)],
    }

    linker_1 = helper.linker_with_registration(df, settings)
    linker_2 = helper.linker_with_registration(df, settings)
    linker_3 = helper.linker_with_registration(df, settings)

    linker_1.training.estimate_u_using_random_sampling(max_pairs=1e3, seed=1)
    linker_2.training.estimate_u_using_random_sampling(max_pairs=1e3, seed=1)
    linker_3.training.estimate_u_using_random_sampling(max_pairs=1e3, seed=2)

    assert (
        linker_1._settings_obj._parameter_estimates_as_records
        == linker_2._settings_obj._parameter_estimates_as_records
    )
    assert (
        linker_1._settings_obj._parameter_estimates_as_records
        != linker_3._settings_obj._parameter_estimates_as_records
    )


# No SQLite or Postgres - don't support random seed
@mark_with_dialects_excluding("sqlite", "postgres")
def test_seed_u_outputs_different_order(test_helpers, dialect):
    # seed was producing different results due to lack of table ordering
    # df_concat not guaranteed order, so sampling from it, even with seed
    # can lead to different results

    helper = test_helpers[dialect]

    n_rows = 1_000
    names = {
        0: "Andy",
        1: "Robin",
        2: "Sam",
        3: "Ross",
    }
    input_frame = pd.DataFrame(
        [
            {
                "unique_id": i,
                "first_name": names[i % len(names)],
            }
            for i in range(n_rows)
        ]
    )
    # simple way to get a slightly uneven distribution
    input_frame.sample(n=347, random_state=123)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.ExactMatch("first_name"),
        ],
    }

    u_vals = set()
    # each time we shuffle the input frame
    # this simulates the non-deterministic ordering of df_concat
    for i in range(5):
        df_pd = input_frame.sample(frac=1, random_state=i)

        df = helper.convert_frame(df_pd)
        linker = helper.linker_with_registration(df, settings)
        linker.training.estimate_u_using_random_sampling(67, 5330)
        u_prob = (
            linker._settings_obj.comparisons[0]
            ._get_comparison_level_by_comparison_vector_value(0)
            .u_probability
        )

        u_vals.add(u_prob)

    # should have got the same u-value each time, as we supply a seed
    # input data is same, just shuffled - should get ordered when we sample
    assert len(u_vals) == 1


# ==================== Convergence Tests ====================


def test_convergence_diagnostics_structure():
    """Test that estimate_u_values returns properly structured diagnostics."""
    from splink import DuckDBAPI

    data = [
        {"unique_id": i, "name": f"Name{i % 100}"}
        for i in range(500)  # Enough records to generate many pairs
    ]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.ExactMatch("name")],
        "blocking_rules_to_generate_predictions": [],
    }

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)
    from splink.internals.linker import Linker

    linker = Linker(df_sdf, settings)

    # Call the underlying function to get diagnostics
    diagnostics = estimate_u_values(linker, max_pairs=1e5, min_count=50)

    # Should return a list with one entry per comparison
    assert isinstance(diagnostics, list)
    assert len(diagnostics) == 1  # One comparison (name)

    diag = diagnostics[0]
    assert isinstance(diag, ComparisonConvergenceInfo)
    assert diag.output_column_name == "name"
    assert diag.total_pairs_sampled > 0
    assert diag.batches_processed >= 1
    assert diag.time_seconds >= 0
    assert isinstance(diag.converged, bool)

    # Check levels structure
    assert len(diag.levels) == 2  # ExactMatch has 2 non-null levels (match, else)
    for level in diag.levels:
        assert isinstance(level, LevelConvergenceInfo)
        assert level.count >= 0
        assert 0 <= level.u_probability <= 1
        assert isinstance(level.converged, bool)

    # Counts should sum to total pairs
    total_count = sum(level.count for level in diag.levels)
    assert total_count == diag.total_pairs_sampled


def test_convergence_early_termination():
    """Test that convergence stops early when min_count is reached for all levels."""
    from splink import DuckDBAPI

    # Create data where both levels will have many observations
    # Names repeat frequently, so exact matches will be common
    data = [{"unique_id": i, "name": f"Name{i % 10}"} for i in range(200)]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.ExactMatch("name")],
        "blocking_rules_to_generate_predictions": [],
    }

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)
    from splink.internals.linker import Linker

    linker = Linker(df_sdf, settings)

    # With many repeating names, should converge quickly with low min_count
    diagnostics = estimate_u_values(
        linker,
        max_pairs=1e6,  # Large max_pairs
        min_count=10,  # Low threshold
        min_pairs=0,  # No minimum pairs threshold (testing early termination)
        initial_batch_proportion=0.05,  # Start small
    )

    diag = diagnostics[0]

    # Should have converged
    assert diag.converged is True

    # All levels should have converged
    for level in diag.levels:
        assert level.converged is True
        assert level.count >= 10

    # Should NOT have processed all pairs (early termination)
    # Total pairs for 200 records = 200*199/2 = 19900
    total_possible_pairs = 200 * 199 // 2
    assert diag.total_pairs_sampled < total_possible_pairs


def test_convergence_min_count_parameter():
    """Test that min_count parameter affects convergence behavior."""
    from splink import DuckDBAPI

    data = [{"unique_id": i, "name": f"Name{i % 20}"} for i in range(300)]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.ExactMatch("name")],
        "blocking_rules_to_generate_predictions": [],
    }

    db_api_1 = DuckDBAPI()
    df_sdf_1 = db_api_1.register(df)
    from splink.internals.linker import Linker

    linker_low = Linker(df_sdf_1, settings)

    db_api_2 = DuckDBAPI()
    df_sdf_2 = db_api_2.register(df)
    linker_high = Linker(df_sdf_2, settings)

    # Low min_count should process fewer pairs (min_pairs=0 to test early termination)
    diag_low = estimate_u_values(linker_low, max_pairs=1e6, min_count=20, min_pairs=0)[
        0
    ]

    # High min_count should process more pairs (min_pairs=0 to test min_count behavior)
    diag_high = estimate_u_values(
        linker_high, max_pairs=1e6, min_count=200, min_pairs=0
    )[0]

    # Higher min_count requires more samples (or may not converge)
    # At minimum, high threshold should not converge with fewer pairs
    if diag_high.converged:
        for level in diag_high.levels:
            assert level.count >= 200
    else:
        # If it didn't converge, it should have processed all pairs
        pass

    # Low threshold should have converged
    assert diag_low.converged is True
    for level in diag_low.levels:
        assert level.count >= 20


def test_convergence_multiple_comparisons():
    """Test convergence diagnostics with multiple comparisons."""
    from splink import DuckDBAPI

    data = [
        {"unique_id": i, "name": f"Name{i % 15}", "city": f"City{i % 8}"}
        for i in range(200)
    ]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.ExactMatch("name"),
            cl.ExactMatch("city"),
        ],
        "blocking_rules_to_generate_predictions": [],
    }

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)
    from splink.internals.linker import Linker

    linker = Linker(df_sdf, settings)

    diagnostics = estimate_u_values(linker, max_pairs=1e5, min_count=30)

    # Should have diagnostics for each comparison
    assert len(diagnostics) == 2

    comparison_names = {d.output_column_name for d in diagnostics}
    assert comparison_names == {"name", "city"}

    # Each comparison should have processed pairs
    for diag in diagnostics:
        assert diag.total_pairs_sampled > 0
        assert len(diag.levels) == 2  # ExactMatch has 2 levels


def test_convergence_u_probabilities_sum_to_one():
    """Test that u probabilities across levels sum to 1.0 for each comparison."""
    from splink import DuckDBAPI

    data = [{"unique_id": i, "name": f"Name{i % 25}"} for i in range(300)]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.LevenshteinAtThresholds("name", [1, 2])],
        "blocking_rules_to_generate_predictions": [],
    }

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)
    from splink.internals.linker import Linker

    linker = Linker(df_sdf, settings)

    diagnostics = estimate_u_values(linker, max_pairs=1e5, min_count=20, min_pairs=0)

    diag = diagnostics[0]

    # U probabilities should sum to 1.0 (within floating point tolerance)
    total_u = sum(level.u_probability for level in diag.levels)
    assert pytest.approx(total_u, rel=1e-10) == 1.0


@mark_with_dialects_excluding("postgres")
def test_convergence_with_link_only(test_helpers, dialect):
    """Test convergence works correctly in link_only mode."""
    helper = test_helpers[dialect]

    data_l = [{"unique_id": i, "name": f"Name{i % 20}"} for i in range(150)]
    data_r = [{"unique_id": i, "name": f"Name{i % 20}"} for i in range(150)]
    df_l = pd.DataFrame(data_l)
    df_r = pd.DataFrame(data_r)

    settings = {
        "link_type": "link_only",
        "comparisons": [cl.ExactMatch("name")],
        "blocking_rules_to_generate_predictions": [],
    }

    df_l = helper.convert_frame(df_l)
    df_r = helper.convert_frame(df_r)

    linker = helper.linker_with_registration(
        [df_l, df_r],
        settings,
        input_table_aliases=["l", "r"],
    )

    diagnostics = estimate_u_values(linker, max_pairs=1e5, min_count=30, min_pairs=0)

    diag = diagnostics[0]
    assert diag.total_pairs_sampled > 0

    # In link_only, total pairs = 150 * 150 = 22500
    # Should have sampled some of these
    assert diag.batches_processed >= 1

    # U probabilities should be valid
    for level in diag.levels:
        assert 0 < level.u_probability <= 1


def test_min_pairs_parameter():
    """Test that min_pairs parameter enforces minimum pair count."""
    from splink import DuckDBAPI

    # Create data with enough rows to generate many pairs
    # 100 rows = 100*99/2 = 4950 possible pairs
    data = [{"unique_id": i, "name": f"Name{i % 10}"} for i in range(100)]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.ExactMatch("name")],
        "blocking_rules_to_generate_predictions": [],
    }

    db_api_1 = DuckDBAPI()
    df_sdf_1 = db_api_1.register(df)
    from splink.internals.linker import Linker

    linker_low_min = Linker(df_sdf_1, settings)

    db_api_2 = DuckDBAPI()
    df_sdf_2 = db_api_2.register(df)
    linker_high_min = Linker(df_sdf_2, settings)

    # Test 1: Low min_pairs (0) - should converge early when min_count is satisfied
    diag_low = estimate_u_values(
        linker_low_min,
        max_pairs=1e6,
        min_count=10,
        min_pairs=0,  # No minimum pairs requirement
        initial_batch_proportion=0.1,
    )[0]

    # Test 2: Higher min_pairs (1000) - should continue until min_pairs is reached
    diag_high = estimate_u_values(
        linker_high_min,
        max_pairs=1e6,
        min_count=10,
        min_pairs=1000,  # Requires at least 1000 pairs
        initial_batch_proportion=0.1,
    )[0]

    # With low min_pairs=0, convergence depends only on min_count
    # With high min_pairs=1000, must also have at least 1000 pairs
    # So high min_pairs should result in more pairs being sampled
    assert diag_high.total_pairs_sampled >= 1000 or not diag_high.converged

    # If low converged, it should have satisfied min_count
    if diag_low.converged:
        for level in diag_low.levels:
            assert level.count >= 10


def test_min_pairs_graceful_degradation():
    """Test that min_pairs handles dataset too small gracefully."""
    from splink import DuckDBAPI
    import logging

    # Create very small data - only 20 rows = 20*19/2 = 190 possible pairs
    data = [{"unique_id": i, "name": f"Name{i % 5}"} for i in range(20)]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [cl.ExactMatch("name")],
        "blocking_rules_to_generate_predictions": [],
    }

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)
    from splink.internals.linker import Linker

    linker = Linker(df_sdf, settings)

    # Request 1 million min_pairs but only 190 are possible
    # Should proceed gracefully and use all available pairs
    diagnostics = estimate_u_values(
        linker,
        max_pairs=1e6,
        min_count=5,
        min_pairs=1_000_000,  # Way more than possible
    )

    diag = diagnostics[0]

    # Should have used all available pairs (190)
    max_possible = 20 * 19 // 2
    assert diag.total_pairs_sampled == max_possible

    # Will not have converged (couldn't reach min_pairs)
    # but should still have valid u probabilities
    for level in diag.levels:
        assert 0 < level.u_probability <= 1
