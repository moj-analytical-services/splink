import duckdb
import numpy as np
import pandas as pd
import pytest

import splink.internals.comparison_library as cl
from splink.internals.estimate_u import _proportion_sample_size_link_only
from splink.internals.pipeline import CTEPipeline
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

    linker = helper.Linker(df_linker, settings, **helper.extra_linker_args())
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

    linker = helper.Linker(
        [df_l, df_r],
        settings,
        input_table_aliases=["l", "r"],
        **helper.extra_linker_args(),
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    # Check that no records are blocked to records in the same source dataset
    check_blocking_sql = """
    SELECT COUNT(*) AS count FROM __splink__blocked_id_pairs
    WHERE substr(join_key_l,1,1) = substr(join_key_r,1,1)
    """

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(check_blocking_sql, "__splink__df_blocked_same_table_count")
    self_table_count = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    result = self_table_count.as_record_dict()

    self_table_count.drop_table_from_database_and_remove_from_cache()
    assert result[0]["count"] == 0

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

    linker = helper.Linker(
        [df_l, df_r],
        settings,
        input_table_aliases=["_a", "_b"],
        **helper.extra_linker_args(),
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    linker.training.estimate_u_using_random_sampling(max_pairs=max_pairs)

    # count how many pairs we _actually_ generated in random sampling
    check_blocking_sql = """
    SELECT COUNT(*) AS count FROM __splink__blocked_id_pairs
    """

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(check_blocking_sql, "__splink__df_blocked_same_table_count")
    self_table_count = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    result = self_table_count.as_record_dict()
    self_table_count.drop_table_from_database_and_remove_from_cache()
    pairs_actually_sampled = result[0]["count"]

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

    linker = helper.Linker(
        dfs,
        settings,
        input_table_aliases=["a", "b", "c", "d"],
        **helper.extra_linker_args(),
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    check_blocking_sql = """
    SELECT COUNT(*) AS count FROM __splink__blocked_id_pairs
    WHERE substr(join_key_l,1,1) = substr(join_key_r,1,1)
    """

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(check_blocking_sql, "__splink__df_blocked_same_table_count")
    self_table_count = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    result = self_table_count.as_record_dict()

    self_table_count.drop_table_from_database_and_remove_from_cache()
    assert result[0]["count"] == 0

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
    linker = helper.Linker(
        dfs,
        settings,
        input_table_aliases=["e", "f", "g", "h"],
        **helper.extra_linker_args(),
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    check_blocking_sql = """
    SELECT COUNT(*) AS count FROM __splink__blocked_id_pairs
    WHERE substr(join_key_l,1,1) = substr(join_key_r,1,1)
    """

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(check_blocking_sql, "__splink__df_blocked_same_table_count")
    self_table_count = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    result = self_table_count.as_record_dict()

    self_table_count.drop_table_from_database_and_remove_from_cache()
    assert result[0]["count"] == (2 * 1 / 2 + 3 * 2 / 2 + 4 * 3 / 2 + 7 * 6 / 2)

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

    linker_1 = helper.Linker(df, settings, **helper.extra_linker_args())
    linker_2 = helper.Linker(df, settings, **helper.extra_linker_args())
    linker_3 = helper.Linker(df, settings, **helper.extra_linker_args())

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
        linker = helper.Linker(df, settings, **helper.extra_linker_args())
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
