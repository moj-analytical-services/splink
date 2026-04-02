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
def test_sql_pipeline_explain_spark_formatted(spark_api):
    _register_input_tables_spark(spark_api)

    explain_result = spark_api.sql_pipeline_to_explain_result(
        _three_step_pipeline(), analyze=False
    )

    assert isinstance(explain_result, str)
    assert "== Physical Plan ==" in explain_result
    assert "BroadcastHashJoin" in explain_result
