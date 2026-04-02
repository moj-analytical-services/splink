import pandas as pd

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.pipeline import CTEPipeline
from splink.internals.spark.database_api import SparkAPI

from .decorator import mark_with_dialects_including


def _register_input_tables(db_api: DuckDBAPI) -> None:
    db_api.register(
        pd.DataFrame(
            [
                {"unique_id": 1, "first_name": "John"},
                {"unique_id": 2, "first_name": "Robin"},
                {"unique_id": 3, "first_name": "Alice"},
            ]
        ),
        source_dataset_name="data_1",
    )
    db_api.register(
        pd.DataFrame(
            [
                {"unique_id": 10, "first_name": "John"},
                {"unique_id": 11, "first_name": "Robin"},
                {"unique_id": 12, "first_name": "Robin"},
                {"unique_id": 13, "first_name": "Zoe"},
            ]
        ),
        source_dataset_name="data_2",
    )


def _three_step_pipeline() -> CTEPipeline:
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(
        """
        select
            l.unique_id as unique_id_l,
            r.unique_id as unique_id_r,
            l.first_name as first_name_l,
            r.first_name as first_name_r
        from data_1 as l
        inner join data_2 as r
            on l.first_name = r.first_name
        """,
        "__splink__blocked_pairs",
    )
    pipeline.enqueue_sql(
        """
        select
            first_name_l,
            count(*) as pair_count
        from __splink__blocked_pairs
        group by first_name_l
        """,
        "__splink__pair_counts_by_name",
    )
    pipeline.enqueue_sql(
        """
        select
            sum(pair_count) as total_pairs,
            count(*) as distinct_names
        from __splink__pair_counts_by_name
        """,
        "__splink__pipeline_summary",
    )
    return pipeline


def _register_input_tables_spark(spark_api: SparkAPI) -> None:
    spark_api.spark.catalog.dropTempView("data_1")
    spark_api.spark.catalog.dropTempView("data_2")

    spark_api.register(
        spark_api.spark.sql(
            """
            SELECT 1 AS unique_id, 'John' AS first_name
            UNION ALL
            SELECT 2 AS unique_id, 'Robin' AS first_name
            UNION ALL
            SELECT 3 AS unique_id, 'Alice' AS first_name
            """
        ),
        source_dataset_name="data_1",
    )
    spark_api.register(
        spark_api.spark.sql(
            """
            SELECT 10 AS unique_id, 'John' AS first_name
            UNION ALL
            SELECT 11 AS unique_id, 'Robin' AS first_name
            UNION ALL
            SELECT 12 AS unique_id, 'Robin' AS first_name
            UNION ALL
            SELECT 13 AS unique_id, 'Zoe' AS first_name
            """
        ),
        source_dataset_name="data_2",
    )


@mark_with_dialects_including("duckdb")
def test_sql_pipeline_explain_analyze_duckdb():
    db_api = DuckDBAPI()
    _register_input_tables(db_api)

    explain_result = db_api.sql_pipeline_to_explain_result(
        _three_step_pipeline(), analyze=True
    )

    assert isinstance(explain_result, str)
    assert "Query Profiling Information" in explain_result
    assert "join data_2 as r" in explain_result
    assert "count(*) as pair_count" in explain_result


@mark_with_dialects_including("duckdb")
def test_sql_pipeline_profiling_duckdb(tmp_path):
    from splink.internals.duckdb.database_api import DuckDBAPIWithProfiling

    db_api = DuckDBAPIWithProfiling(query_profiling_dir=tmp_path)
    _register_input_tables(db_api)

    result = db_api.sql_pipeline_to_splink_dataframe(_three_step_pipeline())

    assert result.as_record_dict() == [{"total_pairs": 3, "distinct_names": 2}]

    profile_paths = list(tmp_path.glob("*.txt"))
    assert len(profile_paths) == 1

    profile_text = profile_paths[0].read_text(encoding="utf-8")
    assert "Query Profiling Information" in profile_text
    assert "__splink__blocked_pairs" in profile_text


@mark_with_dialects_including("duckdb")
def test_sql_pipeline_explain_does_not_profile_duckdb(tmp_path):
    from splink.internals.duckdb.database_api import DuckDBAPIWithProfiling

    db_api = DuckDBAPIWithProfiling(query_profiling_dir=tmp_path)
    _register_input_tables(db_api)

    db_api.sql_pipeline_to_explain_result(_three_step_pipeline(), analyze=False)

    assert list(tmp_path.glob("*.txt")) == []


@mark_with_dialects_including("spark")
def test_sql_pipeline_explain_analyze_spark(spark_api):
    _register_input_tables_spark(spark_api)

    explain_result = spark_api.sql_pipeline_to_explain_result(
        _three_step_pipeline(), analyze=True
    )

    assert isinstance(explain_result, str)
    assert "== Final Physical Plan ==" in explain_result
    assert "== Total Runtime ==" in explain_result
    assert "== Runtime Metrics ==" in explain_result
    assert "BroadcastHashJoin" in explain_result
    assert "HashAggregate" in explain_result
    assert "number of output rows" in explain_result
    assert "duration = " in explain_result
    assert " ms" in explain_result
    assert " ns (" in explain_result


@mark_with_dialects_including("spark")
def test_sql_pipeline_profiling_spark(spark_api, tmp_path):
    from splink.internals.spark.database_api import SparkAPIWithProfiling

    profiling_api = SparkAPIWithProfiling(
        spark_session=spark_api.spark,
        break_lineage_method="persist",
        num_partitions_on_repartition=1,
        query_profiling_dir=tmp_path,
    )
    _register_input_tables_spark(profiling_api)

    result = profiling_api.sql_pipeline_to_splink_dataframe(_three_step_pipeline())

    assert result.as_record_dict() == [{"total_pairs": 3, "distinct_names": 2}]

    profile_paths = list(tmp_path.glob("*.txt"))
    assert len(profile_paths) == 1

    profile_text = profile_paths[0].read_text(encoding="utf-8")
    assert "== Final Physical Plan ==" in profile_text
    assert "== Total Runtime ==" in profile_text
    assert "== Runtime Metrics ==" in profile_text


@mark_with_dialects_including("spark")
def test_sql_pipeline_explain_does_not_profile_spark(spark_api, tmp_path):
    from splink.internals.spark.database_api import SparkAPIWithProfiling

    profiling_api = SparkAPIWithProfiling(
        spark_session=spark_api.spark,
        break_lineage_method="persist",
        num_partitions_on_repartition=1,
        query_profiling_dir=tmp_path,
    )
    _register_input_tables_spark(profiling_api)

    profiling_api.sql_pipeline_to_explain_result(_three_step_pipeline(), analyze=False)

    assert list(tmp_path.glob("*.txt")) == []


@mark_with_dialects_including("spark")
def test_sql_pipeline_explain_spark_formatted(spark_api):
    _register_input_tables_spark(spark_api)

    explain_result = spark_api.sql_pipeline_to_explain_result(
        _three_step_pipeline(), analyze=False
    )

    assert isinstance(explain_result, str)
    assert "== Physical Plan ==" in explain_result
    assert "BroadcastHashJoin" in explain_result
