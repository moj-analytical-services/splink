import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink.backends.duckdb import DuckDBAPIWithProfiling


def test_profiling_with_parquet_materialisation(tmp_path):
    profiles = tmp_path / "profiles"
    api = DuckDBAPIWithProfiling(
        query_profiling_dir=profiles,
        materialisation="parquet",
        materialisation_dir=tmp_path / "backing",
    )

    result = api.query_sql("SELECT 1 AS value")

    assert result.as_record_list() == [{"value": 1}]
    profile_files = list(profiles.glob("*.json"))
    assert len(profile_files) == 1
    assert "COPY" in profile_files[0].read_text()


def test_training_and_prediction_leave_only_owned_parquet_files(tmp_path):
    data = [
        {"unique_id": 1, "name": "Robin", "surname": "Smith", "city": "London"},
        {"unique_id": 2, "name": "Robin", "surname": "Smyth", "city": "London"},
        {"unique_id": 3, "name": "Alice", "surname": "Smith", "city": "London"},
        {"unique_id": 4, "name": "Alice", "surname": "Jones", "city": "London"},
        {"unique_id": 5, "name": "Bob", "surname": "Brown", "city": "Leeds"},
        {"unique_id": 6, "name": "Bobby", "surname": "Brown", "city": "Leeds"},
        {"unique_id": 7, "name": "Carol", "surname": "White", "city": "Leeds"},
        {"unique_id": 8, "name": "Carol", "surname": "Brown", "city": "Leeds"},
    ]
    backing = tmp_path / "backing"
    user_parquet = backing / "user.parquet"
    backing.mkdir()
    user_parquet.write_bytes(b"user data")
    api = DuckDBAPI(
        materialisation="parquet",
        materialisation_dir=backing,
    )
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[cl.ExactMatch("name"), cl.ExactMatch("surname")],
        blocking_rules_to_generate_predictions=[block_on("city")],
        max_iterations=1,
    )
    linker = Linker(api.register(data), settings)

    linker.training.estimate_parameters_using_expectation_maximisation(block_on("city"))
    prediction = linker.inference.predict()

    materialiser = api._get_parquet_materialiser()
    assert prediction.physical_name in materialiser._owned_paths
    assert not materialiser._pending_queries
    tracked_files = {
        file.resolve()
        for path in materialiser._owned_paths.values()
        for file in path.glob("*.parquet")
    }
    files_on_disk = {
        file.resolve() for file in backing.rglob("*.parquet") if file != user_parquet
    }
    assert tracked_files
    assert files_on_disk == tracked_files

    api.delete_tables_created_by_splink_from_db()
    assert user_parquet.read_bytes() == b"user data"
    assert list(backing.rglob("*.parquet")) == [user_parquet]
