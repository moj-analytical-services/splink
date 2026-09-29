import pytest

import splink.comparison_level_library as cll
import splink.comparison_library as cl
import splink.internals.expectation_maximisation as em
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink.internals.em_training_session import EMTrainingSession
from splink.internals.exceptions import EMTrainingException
from tests.decorator import mark_with_dialects_including


@mark_with_dialects_including("duckdb")
@pytest.mark.parametrize("link_type", ["dedupe_only", "link_only"])
@pytest.mark.parametrize("debug_mode", [False, True])
def test_precomputed_tf_table_contains_only_training_columns(
    fake_1000, monkeypatch, link_type, debug_mode
):
    db_api = DuckDBAPI()
    data = fake_1000.slice(0, 20)
    if link_type == "link_only":
        inputs = [
            db_api.register(data.slice(0, 10)),
            db_api.register(data.slice(10, 10)),
        ]
        expected_pairs = 100
    else:
        inputs = db_api.register(data)
        expected_pairs = 190
    linker = Linker(
        inputs,
        SettingsCreator(
            link_type=link_type,
            comparisons=[
                cl.ExactMatch("first_name").configure(term_frequency_adjustments=True),
                cl.ExactMatch("surname"),
            ],
            blocking_rules_to_generate_predictions=["1 = 1"],
            max_iterations=2,
        ),
    )
    db_api.debug_mode = debug_mode
    execute = db_api.sql_pipeline_to_splink_dataframe
    prepared_columns = []

    def capture_prepared_table(pipeline):
        df = execute(pipeline)
        if df.templated_name == "__splink__df_comparison_vectors_with_tf":
            prepared_columns.append({col.unquote().name for col in df.columns})
            assert len(df.as_record_list()) == expected_pairs
        return df

    monkeypatch.setattr(
        db_api, "sql_pipeline_to_splink_dataframe", capture_prepared_table
    )
    linker.training.estimate_parameters_using_expectation_maximisation("1 = 1")
    assert prepared_columns == [
        {"gamma_first_name", "gamma_surname", "mw_tf_adj_first_name"}
    ]
    queries = db_api._intermediate_table_cache.executed_queries
    # Debug mode deliberately materialises each stage of the pipeline.
    assert (
        any(df.templated_name == "__splink__df_comparison_vectors" for df in queries)
        == debug_mode
    )

    predictions = linker.inference.predict()
    prediction_columns = {col.unquote().name for col in predictions.columns}
    assert {"unique_id_l", "unique_id_r", "match_key"} <= prediction_columns
    if link_type == "link_only":
        assert {"source_dataset_l", "source_dataset_r"} <= prediction_columns


@mark_with_dialects_including("duckdb")
@pytest.mark.parametrize(
    "disable_exact_match_detection, tf_weight, tf_frequency",
    [
        pytest.param(False, 0.5, 0.1, id="fractional-weight"),
        pytest.param(False, 0.5, 0.001, id="frequency-floor"),
        pytest.param(True, 0.5, 0.1, id="exact-match-detection-disabled"),
        pytest.param(False, 0.0, 0.1, id="zero-weight"),
    ],
)
def test_precomputed_tf_matches_iteration_history(
    fake_1000, disable_exact_match_detection, tf_weight, tf_frequency
):
    histories = []
    for fix_u in (False, True):
        db_api = DuckDBAPI()
        settings = SettingsCreator(
            link_type="dedupe_only",
            comparisons=[
                cl.CustomComparison(
                    comparison_levels=[
                        cll.NullLevel("first_name"),
                        cll.ExactMatchLevel("first_name").configure(
                            tf_adjustment_column="first_name",
                            u_probability=0.1,
                            fix_u_probability=True,
                        ),
                        cll.LevenshteinLevel("first_name", 1).configure(
                            tf_adjustment_column="first_name",
                            tf_adjustment_weight=tf_weight,
                            tf_minimum_u_value=0.02,
                            disable_tf_exact_match_detection=(
                                disable_exact_match_detection
                            ),
                            u_probability=0.2,
                            fix_u_probability=True,
                        ),
                        cll.ElseLevel().configure(
                            u_probability=0.7, fix_u_probability=True
                        ),
                    ],
                    output_column_name="first_name",
                ),
                cl.ExactMatch("surname").configure(term_frequency_adjustments=True),
                cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            ],
            max_iterations=4,
            em_convergence=0,
        )
        linker = Linker(db_api.register(fake_1000), settings)
        for cc in linker._settings_obj.comparisons:
            for level in cc.comparison_levels:
                level._fix_u_probability = True
        # "Jack" vs "Jack " exercises fuzzy TF; other names have no lookup entry.
        lookup = db_api.register(
            [{"first_name": "Jack", "tf_first_name": tf_frequency}]
        )
        linker.table_management.register_term_frequency_lookup(lookup, "first_name")
        session = linker.training.estimate_parameters_using_expectation_maximisation(
            block_on("city"), fix_u_probabilities=fix_u
        )
        histories.append(session._core_model_settings_history)
        queries = db_api._intermediate_table_cache.executed_queries
        if not fix_u:
            vectors = next(
                df
                for df in queries
                if df.templated_name == "__splink__df_comparison_vectors"
            )
            fuzzy_pairs_with_tf = db_api.duckdb_con.sql(
                f"""
                select count(*) from {vectors.physical_name}
                where gamma_first_name = 1
                and coalesce(tf_first_name_l, tf_first_name_r) is not null
                """
            ).fetchone()[0]
            assert fuzzy_pairs_with_tf > 0
        prepared = [
            df
            for df in queries
            if df.templated_name == "__splink__df_comparison_vectors_with_tf"
        ]
        assert len(prepared) == int(fix_u)
        iterations = [
            df for df in queries if df.templated_name == "__splink__m_u_counts"
        ]
        assert len(iterations) == 4
        for df in iterations:
            assert ("log2(" in df.sql_used_to_create) == (not fix_u)
        if fix_u:
            assert prepared[0].physical_name not in db_api._created_tables
            tables = db_api.duckdb_con.sql("show tables").fetchall()
            assert (prepared[0].physical_name,) not in tables

    # Per-level fixed u gives the uncached path the same updates as global fixed u.
    assert len(histories[0]) == len(histories[1]) == 5
    for expected, actual in zip(*histories):
        assert actual.probability_two_random_records_match == pytest.approx(
            expected.probability_two_random_records_match, rel=1e-10, abs=1e-12
        )
        for expected_cc, actual_cc in zip(expected.comparisons, actual.comparisons):
            for expected_cl, actual_cl in zip(
                expected_cc._comparison_levels_excluding_null,
                actual_cc._comparison_levels_excluding_null,
            ):
                assert actual_cl.m_probability == pytest.approx(
                    expected_cl.m_probability, rel=1e-10, abs=1e-12
                )
                assert actual_cl.u_probability == expected_cl.u_probability


@mark_with_dialects_including("duckdb")
@pytest.mark.parametrize(
    "without_tf, fix_u, tf_enabled",
    [(True, True, True), (False, False, True), (False, True, False)],
)
def test_tf_precomputation_not_used(fake_1000, without_tf, fix_u, tf_enabled):
    db_api = DuckDBAPI()
    linker = Linker(
        db_api.register(fake_1000),
        SettingsCreator(
            link_type="dedupe_only",
            comparisons=[
                cl.ExactMatch("first_name").configure(
                    term_frequency_adjustments=tf_enabled
                ),
                cl.ExactMatch("surname"),
                # This comparison is removed by the training block.
                cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            ],
            max_iterations=2,
        ),
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("city"),
        estimate_without_term_frequencies=without_tf,
        fix_u_probabilities=fix_u,
    )
    assert all(
        df.templated_name != "__splink__df_comparison_vectors_with_tf"
        for df in db_api._intermediate_table_cache.executed_queries
    )


@mark_with_dialects_including("duckdb")
def test_precomputed_tf_cleanup_on_failure(fake_1000, monkeypatch):
    db_api = DuckDBAPI()
    linker = Linker(
        db_api.register(fake_1000),
        SettingsCreator(
            link_type="dedupe_only",
            comparisons=[
                cl.ExactMatch("first_name").configure(term_frequency_adjustments=True),
                cl.ExactMatch("surname"),
                cl.ExactMatch("city"),
            ],
        ),
    )

    def fail(*args, **kwargs):
        raise RuntimeError("EM failed")

    monkeypatch.setattr(em, "maximisation_step", fail)
    with pytest.raises(RuntimeError, match="EM failed"):
        linker.training.estimate_parameters_using_expectation_maximisation(
            block_on("city")
        )
    tables = db_api.duckdb_con.sql("show tables").fetchall()
    assert not any("comparison_vectors_with_tf" in name for (name,) in tables)
    assert not any("df_comparison_vectors" in name for (name,) in tables)


@mark_with_dialects_including("duckdb")
@pytest.mark.parametrize("fail_training", [False, True])
def test_supplied_comparison_vectors_are_preserved(
    fake_1000, monkeypatch, fail_training
):
    db_api = DuckDBAPI()
    linker = Linker(
        db_api.register(fake_1000.slice(0, 20)),
        SettingsCreator(
            link_type="dedupe_only",
            comparisons=[
                cl.ExactMatch("first_name").configure(term_frequency_adjustments=True),
                cl.ExactMatch("surname"),
            ],
            max_iterations=2,
        ),
    )
    settings = linker._settings_obj
    session = EMTrainingSession(
        linker,
        db_api=db_api,
        blocking_rule_for_training="1 = 1",
        core_model_settings=settings.core_model_settings.copy(),
        training_settings=settings.training_settings,
        unique_id_input_columns=settings.column_info_settings.unique_id_input_columns,
        fix_u_probabilities=True,
    )
    cvv = session._comparison_vectors()
    records = cvv.as_record_list()
    if fail_training:

        def fail(*args, **kwargs):
            raise RuntimeError("EM failed")

        monkeypatch.setattr(em, "maximisation_step", fail)
        with pytest.raises(RuntimeError, match="EM failed"):
            session._train(cvv)
    else:
        session._train(cvv)
        assert len(session._core_model_settings_history) == 3

    assert cvv.as_record_list() == records
    prepared = [
        df
        for df in db_api._intermediate_table_cache.executed_queries
        if df.templated_name == "__splink__df_comparison_vectors_with_tf"
    ]
    assert len(prepared) == 1
    tables = db_api.duckdb_con.sql("show tables").fetchall()
    assert (prepared[0].physical_name,) not in tables
    assert prepared[0].physical_name not in db_api._created_tables


@mark_with_dialects_including("duckdb")
def test_precomputed_tf_cleanup_on_empty_block():
    db_api = DuckDBAPI()
    linker = Linker(
        db_api.register(
            [
                {"unique_id": 1, "first_name": "Jane", "surname": "Smith"},
                {"unique_id": 2, "first_name": "Jane", "surname": "Jones"},
            ]
        ),
        SettingsCreator(
            link_type="dedupe_only",
            comparisons=[
                cl.ExactMatch("first_name").configure(term_frequency_adjustments=True),
                cl.ExactMatch("surname"),
            ],
        ),
    )
    with pytest.raises(EMTrainingException, match="resulted in no record pairs"):
        linker.training.estimate_parameters_using_expectation_maximisation(
            block_on("surname")
        )
    tables = db_api.duckdb_con.sql("show tables").fetchall()
    assert not any("comparison_vectors" in name for (name,) in tables)
    assert not any("comparison_vectors" in name for name in db_api._created_tables)
