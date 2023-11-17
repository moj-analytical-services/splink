import numpy as np
import pandas as pd
import pytest

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
        "comparisons": [helper.cl.levenshtein_at_thresholds("name", 2)],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }
    df_linker = helper.convert_frame(df)

    linker = helper.Linker(df_linker, settings, **helper.extra_linker_args())
    linker.debug_mode = True
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
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


@mark_with_dialects_excluding()
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
        "comparisons": [helper.cl.levenshtein_at_thresholds("name", 2)],
        "blocking_rules_to_generate_predictions": [],
    }

    df_l = helper.convert_frame(df_l)
    df_r = helper.convert_frame(df_r)

    linker = helper.Linker([df_l, df_r], settings, **helper.extra_linker_args())
    linker.debug_mode = True
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    check_blocking_sql = """
    SELECT COUNT(*) AS count FROM __splink__df_blocked
    WHERE source_dataset_l = source_dataset_r
    """
    self_table_count = linker._sql_to_splink_dataframe_checking_cache(
        check_blocking_sql, "__splink__df_blocked_same_table_count"
    )

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


@mark_with_dialects_excluding()
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
    # levenshtein should be on string types
    df_l["name"] = df_l["name"].astype("str")
    df_r["name"] = df_r["name"].astype("str")

    # max_pairs is a good deal less than total possible pairs = 9_000_000
    max_pairs = 1_800_000

    settings = {
        "link_type": "link_only",
        "comparisons": [helper.cl.levenshtein_at_thresholds("name", 2)],
        "blocking_rules_to_generate_predictions": [],
    }

    df_l = helper.convert_frame(df_l)
    df_r = helper.convert_frame(df_r)

    linker = helper.Linker([df_l, df_r], settings, **helper.extra_linker_args())
    linker.debug_mode = True
    linker.estimate_u_using_random_sampling(max_pairs=max_pairs)

    # count how many pairs we _actually_ generated in random sampling
    check_blocking_sql = """
    SELECT COUNT(*) AS count FROM __splink__df_blocked
    """
    self_table_count = linker._sql_to_splink_dataframe_checking_cache(
        check_blocking_sql, "__splink__df_blocked_same_table_count"
    )

    result = self_table_count.as_record_dict()
    self_table_count.drop_table_from_database_and_remove_from_cache()
    pairs_actually_sampled = result[0]["count"]

    proportion_of_max_pairs_sampled = pairs_actually_sampled / max_pairs
    # proportion_of_max_pairs_sampled should be 1 - i.e. we sample max_pairs rows
    # as we have many more pairs available than max_pairs
    # equality only holds probabilistically for some backends, due to sampling strategy
    # chance of failure is approximately 1e-06 with this choice of relative error
    assert pytest.approx(proportion_of_max_pairs_sampled, rel=0.15) == 1.0


@mark_with_dialects_excluding()
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
        "comparisons": [helper.cl.levenshtein_at_thresholds("name", 2)],
        "blocking_rules_to_generate_predictions": [],
    }

    linker = helper.Linker(dfs, settings, **helper.extra_linker_args())
    linker.debug_mode = True
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    check_blocking_sql = """
    SELECT COUNT(*) AS count FROM __splink__df_blocked
    WHERE source_dataset_l = source_dataset_r
    """

    self_table_count = linker._sql_to_splink_dataframe_checking_cache(
        check_blocking_sql, "__splink__df_blocked_same_table_count"
    )

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
    linker = helper.Linker(dfs, settings, **helper.extra_linker_args())
    linker.debug_mode = True
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    cc_name = linker._settings_obj.comparisons[0]

    check_blocking_sql = """
    SELECT COUNT(*) AS count FROM __splink__df_blocked
    WHERE source_dataset_l = source_dataset_r
    """

    self_table_count = linker._sql_to_splink_dataframe_checking_cache(
        check_blocking_sql, "__splink__df_blocked_same_table_count"
    )

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
        "comparisons": [helper.cl.levenshtein_at_thresholds("first_name", 2)],
    }

    linker_1 = helper.Linker(df, settings, **helper.extra_linker_args())
    linker_2 = helper.Linker(df, settings, **helper.extra_linker_args())
    linker_3 = helper.Linker(df, settings, **helper.extra_linker_args())

    linker_1.estimate_u_using_random_sampling(max_pairs=1e3, seed=1)
    linker_2.estimate_u_using_random_sampling(max_pairs=1e3, seed=1)
    linker_3.estimate_u_using_random_sampling(max_pairs=1e3, seed=2)

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
def test_seed_u_outputs_complex_model(test_helpers, dialect):
    # seed was producing different results due to lack of table ordering
    # only with sufficiently 'complex' models
    # have not been able to recreate with fake_1000 (even if replicated multiple times)
    helper = test_helpers[dialect]

    from splink.datasets import splink_datasets
    df = helper.convert_frame(splink_datasets.historical_50k)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            helper.ctl.name_comparison("first_name", term_frequency_adjustments=True),
        ],
    }

    u_vals = set()
    for _ in range(15):
        linker = helper.Linker(df, settings, **helper.extra_linker_args())
        linker.estimate_u_using_random_sampling(1000, 5330)
        u_prob = (linker
                ._settings_obj.comparisons[0]
                ._get_comparison_level_by_comparison_vector_value(0)
                .u_probability
        )
        u_vals.add(u_prob)
    print(u_vals)  # NOQA
    # should have got the same u-value each time, as we supply a seed
    assert len(u_vals) == 1
