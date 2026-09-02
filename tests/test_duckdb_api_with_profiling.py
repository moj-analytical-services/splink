import json
import re
from datetime import datetime, timezone

import duckdb
import pytest

from splink.backends.duckdb import DuckDBAPIWithProfiling
from splink.internals.duckdb import database_api_with_profiling
from splink.internals.exceptions import SplinkException


@pytest.mark.parametrize(
    ("profiling_format", "extension"),
    [("text", "txt"), ("json", "json")],
)
def test_profiles_actual_ctas_once(tmp_path, profiling_format, extension):
    connection = duckdb.connect()
    connection.execute("CREATE SEQUENCE execution_count")
    db_api = DuckDBAPIWithProfiling(
        connection=connection,
        query_profiling_dir=tmp_path,
        profiling_format=profiling_format,
    )

    db_api._sql_to_splink_dataframe(
        "SELECT nextval('execution_count') AS execution_number",
        "first_query",
        "profiled_first_table",
    )
    db_api._sql_to_splink_dataframe(
        "SELECT nextval('execution_count') AS execution_number",
        "second_query",
        "profiled_second_table",
    )

    assert connection.execute("SELECT nextval('execution_count')").fetchone()[0] == 3

    profile_paths = sorted(tmp_path.iterdir())
    assert len(profile_paths) == 2
    assert re.fullmatch(
        rf"\d{{8}}T\d{{6}}Z_0001_profiled_first_table_duckdb\.{extension}",
        profile_paths[0].name,
    )
    assert re.fullmatch(
        rf"\d{{8}}T\d{{6}}Z_0002_profiled_second_table_duckdb\.{extension}",
        profile_paths[1].name,
    )

    if profiling_format == "json":
        profiles = [json.loads(path.read_text()) for path in profile_paths]
        profile_queries = [profile["query_name"] for profile in profiles]
    else:
        profile_queries = [path.read_text() for path in profile_paths]

    assert "CREATE TABLE profiled_first_table AS" in profile_queries[0]
    assert "CREATE TABLE profiled_second_table AS" in profile_queries[1]


def test_rejects_unknown_profiling_format(tmp_path):
    with pytest.raises(ValueError, match="profiling_format"):
        DuckDBAPIWithProfiling(
            query_profiling_dir=tmp_path,
            profiling_format="yaml",
        )


def test_disables_profiling_after_query_failure(tmp_path):
    connection = duckdb.connect()
    db_api = DuckDBAPIWithProfiling(
        connection=connection,
        query_profiling_dir=tmp_path,
        profiling_format="json",
    )

    with pytest.raises(SplinkException):
        db_api._sql_to_splink_dataframe(
            "SELECT * FROM table_that_does_not_exist",
            "failing_query",
            "failing_table",
        )

    profiling_setting = connection.execute(
        "SELECT current_setting('enable_profiling')"
    ).fetchone()[0]
    assert profiling_setting is None


def test_supports_output_schema(tmp_path):
    db_api = DuckDBAPIWithProfiling(
        output_schema="profiled_schema",
        query_profiling_dir=tmp_path,
    )

    output = db_api._sql_to_splink_dataframe(
        "SELECT 1 AS value",
        "schema_query",
        "schema_table",
    )

    assert output.as_record_list() == [{"value": 1}]
    assert len(list(tmp_path.iterdir())) == 1


def test_avoids_existing_profile_filename(tmp_path, monkeypatch):
    class FixedDatetime:
        @classmethod
        def now(cls, tz):
            return datetime(2026, 9, 2, 12, 34, 56, tzinfo=timezone.utc)

    monkeypatch.setattr(database_api_with_profiling, "datetime", FixedDatetime)

    for _ in range(2):
        db_api = DuckDBAPIWithProfiling(query_profiling_dir=tmp_path)
        db_api._sql_to_splink_dataframe(
            "SELECT 1 AS value",
            "repeated_query",
            "repeated_table",
        )

    assert [path.name for path in sorted(tmp_path.iterdir())] == [
        "20260902T123456Z_0001_repeated_table_duckdb.txt",
        "20260902T123456Z_0002_repeated_table_duckdb.txt",
    ]
