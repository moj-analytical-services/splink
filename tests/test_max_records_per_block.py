import pandas as pd
import pytest

import splink.comparison_library as cl
from splink import SettingsCreator, block_on
from splink.internals.pipeline import CTEPipeline
from tests.decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_max_records_per_block_caps_pairs(test_helpers, dialect):
    """Verify max_records_per_block actually limits pair generation."""
    helper = test_helpers[dialect]

    # Create data with one mega-block (all same first_name)
    df = pd.DataFrame(
        {
            "unique_id": range(200),
            "first_name": ["John"] * 200,  # All same = mega-block
            "surname": [f"Smith{i}" for i in range(200)],  # All different
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
        blocking_rules_to_generate_predictions=[],
    )

    linker = helper.Linker(df, settings, **helper.extra_linker_args())

    # Enable debug mode to keep temp views for inspection
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    # Without cap: 200*199/2 = 19,900 pairs for first_name block
    # With cap of 10: significantly fewer pairs due to dual-sided capping
    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        "l.first_name = r.first_name",
        max_records_per_block=10,
    )

    assert em_session is not None

    # Query the blocked pairs table to verify capping worked
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    max_uncapped_pairs = (200 * 199) // 2  # 19,900

    # Verify significant reduction (at least 50% reduction)
    assert pair_count < max_uncapped_pairs * 0.5, (
        f"Expected significant pair reduction with max_records_per_block=10. "
        f"Got {pair_count} pairs, uncapped would be {max_uncapped_pairs}"
    )

    # Also verify we still have SOME pairs (capping didn't eliminate everything)
    assert pair_count > 0, "Capping should not eliminate all pairs"


@mark_with_dialects_excluding()
def test_max_records_per_block_none_is_unlimited(test_helpers, dialect):
    """Verify None means no limit (backward compatible)."""
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        {
            "unique_id": range(20),
            "first_name": ["John"] * 20,
            "surname": [f"Smith{i}" for i in range(20)],
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    # Should work without max_records_per_block (default None)
    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        "l.first_name = r.first_name",
    )

    assert em_session is not None

    # Verify all pairs were generated (no capping)
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    expected_pairs = 20 * 19 // 2  # 190 pairs
    assert pair_count == expected_pairs, (
        f"Without capping, should have all pairs. "
        f"Expected {expected_pairs}, got {pair_count}"
    )


@mark_with_dialects_excluding()
def test_max_records_per_block_with_block_on(test_helpers, dialect):
    """Verify works with block_on() helper."""
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        {
            "unique_id": range(100),
            "city": ["London"] * 100,
            "postcode": ["SW1A"] * 100,
            "surname": [f"Smith{i}" for i in range(100)],
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("city", "postcode"),
        max_records_per_block=10,
    )

    assert em_session is not None

    # Verify capping reduced pairs
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    max_uncapped = 100 * 99 // 2  # 4950 pairs
    assert (
        pair_count < max_uncapped
    ), f"Expected capping to reduce pairs from {max_uncapped}"


@mark_with_dialects_excluding()
def test_max_records_per_block_deterministic(test_helpers, dialect):
    """Verify results are deterministic with the same data and cap."""
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        {
            "unique_id": range(100),
            "group": ["A"] * 100,
            "surname": [f"Smith{i}" for i in range(100)],
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    linker1 = helper.Linker(df, settings, **helper.extra_linker_args())
    linker2 = helper.Linker(df, settings, **helper.extra_linker_args())

    # Both should produce same results with same cap
    em1 = linker1.training.estimate_parameters_using_expectation_maximisation(
        "l.group = r.group",
        max_records_per_block=20,
    )
    em2 = linker2.training.estimate_parameters_using_expectation_maximisation(
        "l.group = r.group",
        max_records_per_block=20,
    )

    assert em1 is not None
    assert em2 is not None

    # Compare actual parameter estimates - should be identical
    # The ORDER BY in ROW_NUMBER uses join_key_l/r which are deterministic
    assert (
        linker1._settings_obj._parameter_estimates_as_records
        == linker2._settings_obj._parameter_estimates_as_records
    ), "Parameter estimates should be identical for same data and cap"


@mark_with_dialects_excluding()
def test_max_records_per_block_with_multiple_blocks(test_helpers, dialect):
    """Verify capping works correctly when there are multiple distinct blocks."""
    helper = test_helpers[dialect]

    # Create data with two blocks of different sizes
    df = pd.DataFrame(
        {
            "unique_id": range(150),
            "city": ["London"] * 100 + ["Paris"] * 50,  # Two blocks
            "surname": [f"Smith{i}" for i in range(150)],
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    # With cap: London block capped, Paris block may or may not be capped
    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        "l.city = r.city",
        max_records_per_block=20,
    )

    assert em_session is not None

    # Verify capping reduced pairs
    # Uncapped: London 100*99/2=4950 + Paris 50*49/2=1225 = 6175
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    max_uncapped = (100 * 99 // 2) + (50 * 49 // 2)  # 6175
    assert (
        pair_count < max_uncapped
    ), f"Expected capping to reduce pairs from {max_uncapped}"


@mark_with_dialects_excluding()
def test_max_records_per_block_validation(test_helpers, dialect):
    """Test that invalid max_records_per_block values raise appropriate errors."""
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        {
            "unique_id": range(50),
            "first_name": ["John"] * 50,
            "surname": [f"Smith{i}" for i in range(50)],
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    linker = helper.Linker(df, settings, **helper.extra_linker_args())

    # Test value of 0 raises ValueError
    with pytest.raises(ValueError, match="must be at least 2"):
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.first_name = r.first_name",
            max_records_per_block=0,
        )

    # Test value of 1 raises ValueError (cannot generate pairs with only 1 record)
    with pytest.raises(ValueError, match="must be at least 2"):
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.first_name = r.first_name",
            max_records_per_block=1,
        )

    # Test negative value raises ValueError
    with pytest.raises(ValueError, match="must be at least 2"):
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.first_name = r.first_name",
            max_records_per_block=-5,
        )

    # Test float type raises TypeError
    with pytest.raises(TypeError, match="must be an integer"):
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.first_name = r.first_name",
            max_records_per_block=10.5,
        )

    # Test string type raises TypeError
    with pytest.raises(TypeError, match="must be an integer"):
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.first_name = r.first_name",
            max_records_per_block="10",
        )


@mark_with_dialects_excluding()
def test_max_records_per_block_link_only(test_helpers, dialect):
    """Verify max_records_per_block works with link_type='link_only' (two datasets)."""
    helper = test_helpers[dialect]

    data_l = pd.DataFrame(
        {
            "unique_id": range(100),
            "city": ["London"] * 100,
            "surname": [f"Smith{i}" for i in range(100)],
        }
    )
    data_r = pd.DataFrame(
        {
            "unique_id": range(100),
            "city": ["London"] * 100,
            "surname": [f"Jones{i}" for i in range(100)],
        }
    )

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    df_l = helper.convert_frame(data_l)
    df_r = helper.convert_frame(data_r)

    linker = helper.Linker(
        [df_l, df_r],
        settings,
        input_table_aliases=["df_left", "df_right"],
        **helper.extra_linker_args(),
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        "l.city = r.city",
        max_records_per_block=15,
    )

    assert em_session is not None

    # Verify capping reduced pairs (link_only: 100 * 100 = 10,000 pairs uncapped)
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    max_uncapped = 100 * 100  # 10,000 pairs for link_only
    assert (
        pair_count < max_uncapped
    ), f"Expected capping to reduce pairs from {max_uncapped}"


@mark_with_dialects_excluding()
def test_max_records_per_block_link_and_dedupe(test_helpers, dialect):
    """Verify max_records_per_block works with link_type='link_and_dedupe'."""
    helper = test_helpers[dialect]

    data_l = pd.DataFrame(
        {
            "unique_id": range(50),
            "city": ["London"] * 50,
            "surname": [f"Smith{i}" for i in range(50)],
        }
    )
    data_r = pd.DataFrame(
        {
            "unique_id": range(50),
            "city": ["London"] * 50,
            "surname": [f"Jones{i}" for i in range(50)],
        }
    )

    settings = SettingsCreator(
        link_type="link_and_dedupe",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    df_l = helper.convert_frame(data_l)
    df_r = helper.convert_frame(data_r)

    linker = helper.Linker(
        [df_l, df_r],
        settings,
        input_table_aliases=["df_left", "df_right"],
        **helper.extra_linker_args(),
    )
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        "l.city = r.city",
        max_records_per_block=10,
    )

    assert em_session is not None

    # Verify capping reduced pairs
    # link_and_dedupe: 50*49/2 + 50*49/2 + 50*50 = 1225 + 1225 + 2500 = 4950
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    max_uncapped = (50 * 49 // 2) + (50 * 49 // 2) + (50 * 50)  # 4950
    assert (
        pair_count < max_uncapped
    ), f"Expected capping to reduce pairs from {max_uncapped}"


@mark_with_dialects_excluding()
def test_max_records_per_block_with_salted_blocking_rule(test_helpers, dialect):
    """Verify max_records_per_block works with SaltedBlockingRule."""
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        {
            "unique_id": range(150),
            "city": ["London"] * 150,
            "surname": [f"Smith{i}" for i in range(150)],
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    # Use salted blocking rule with max_records_per_block
    # block_on with salting_partitions creates a SaltedBlockingRule
    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("city", salting_partitions=3),
        max_records_per_block=20,
    )

    assert em_session is not None

    # Verify capping reduced pairs (150*149/2 = 11,175 uncapped)
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    max_uncapped = 150 * 149 // 2  # 11,175 pairs
    assert (
        pair_count < max_uncapped
    ), f"Expected capping to reduce pairs from {max_uncapped}"


@mark_with_dialects_excluding()
def test_max_records_per_block_small_block_unaffected(test_helpers, dialect):
    """Verify blocks smaller than max_records_per_block generate all pairs."""
    helper = test_helpers[dialect]

    # Create small block (5 records) with high cap (100)
    df = pd.DataFrame(
        {
            "unique_id": range(5),
            "first_name": ["John"] * 5,
            "surname": [f"Smith{i}" for i in range(5)],
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    # Cap is 100, but block only has 5 records - should be unaffected
    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        "l.first_name = r.first_name",
        max_records_per_block=100,
    )

    assert em_session is not None

    # Should have all 5*4/2 = 10 pairs since cap is not reached
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    expected_pairs = 5 * 4 // 2  # 10
    assert pair_count == expected_pairs, (
        f"Small block should have all pairs. "
        f"Expected {expected_pairs}, got {pair_count}"
    )


@mark_with_dialects_excluding()
def test_max_records_per_block_minimum_valid_value(test_helpers, dialect):
    """Verify max_records_per_block=2 (minimum valid value) works correctly."""
    helper = test_helpers[dialect]

    df = pd.DataFrame(
        {
            "unique_id": range(50),
            "first_name": ["John"] * 50,
            "surname": [f"Smith{i}" for i in range(50)],
        }
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("surname"),
        ],
    )

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker._debug_mode = True
    linker._db_api.debug_keep_temp_views = True

    # max_records_per_block=2 is the minimum valid value
    # Should work but produce very few pairs
    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        "l.first_name = r.first_name",
        max_records_per_block=2,
    )

    assert em_session is not None

    # With max_records_per_block=2, pairs should be drastically reduced
    count_sql = "SELECT COUNT(*) AS pair_count FROM __splink__blocked_id_pairs"
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(count_sql, "__splink__pair_count_result")
    result_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = result_df.as_record_dict()
    result_df.drop_table_from_database_and_remove_from_cache()

    pair_count = result[0]["pair_count"]
    max_uncapped = 50 * 49 // 2  # 1225
    assert pair_count < max_uncapped * 0.1, (
        f"With max_records_per_block=2, expected <10% of uncapped pairs. "
        f"Got {pair_count}, uncapped would be {max_uncapped}"
    )
