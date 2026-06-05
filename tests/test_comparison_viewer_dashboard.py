import os

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker
from splink.internals.comparison_vector_distribution import (
    comparison_vector_distribution_sql,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_comparison_viewer import (
    comparison_viewer_table_sqls,
)

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_including


@mark_with_dialects_including("duckdb")
def test_comparison_viewer_dashboard(tmp_path, fake_1000):
    settings_dict = get_settings_dict()

    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(
        df_sdf,
        settings=settings_dict,
    )

    df_predict = linker.inference.predict()

    linker.visualisations.comparison_viewer_dashboard(
        df_predict,
        os.path.join(tmp_path, "test_scv.html"),
        overwrite=True,
        num_example_rows=5,
        minimum_comparison_vector_count=10,
    )


@mark_with_dialects_including("duckdb")
def test_comparison_viewer_table():
    # contrived input data to get 10 name agreements
    # and 5 name disagreements.
    data = {
        "unique_id": [1, 2, 3, 4, 5, 6],
        "first_name": [
            "JOHN",
            "JOHN",
            "JOHN",
            "JOHN",
            "JOHN",
            "HENRY",
        ],
    }

    settings_dict = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.ExactMatch("first_name"),
        ],
        "blocking_rules_to_generate_predictions": [
            "1=1",
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    db_api = DuckDBAPI()
    df_sdf = db_api.register(data)

    linker = Linker(
        df_sdf,
        settings=settings_dict,
    )

    df_predict = linker.inference.predict()

    pipeline = CTEPipeline([df_predict])
    sql = comparison_vector_distribution_sql(linker)
    pipeline.enqueue_sql(sql, "__splink__df_comparison_vector_distribution")

    sqls = comparison_viewer_table_sqls(
        linker,
        4,
        minimum_comparison_vector_count=0,
    )
    pipeline.enqueue_list_of_sqls(sqls)

    df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = df.query_sql(
        """
        SELECT
            gamma_first_name,
            count,
            count(*) AS value_count
        FROM
            {this}
        GROUP BY
            gamma_first_name, count
        ORDER BY
            gamma_first_name
        """
    ).as_dict()

    correct_result = {
        "gamma_first_name": [0, 1],
        "count": [5, 10],
        "value_count": [4, 4],
    }

    assert result == correct_result

    # now test with a higher minimum

    pipeline = CTEPipeline([df_predict])
    sql = comparison_vector_distribution_sql(linker)
    pipeline.enqueue_sql(sql, "__splink__df_comparison_vector_distribution")

    sqls = comparison_viewer_table_sqls(
        linker,
        4,
        minimum_comparison_vector_count=7,
    )
    pipeline.enqueue_list_of_sqls(sqls)

    df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    result = df.query_sql(
        """
        SELECT
            gamma_first_name,
            count,
            count(*) AS value_count
        FROM
            {this}
        GROUP BY
            gamma_first_name, count
        ORDER BY
            gamma_first_name
        """
    ).as_dict()

    correct_result = {
        "gamma_first_name": [1],
        "count": [10],
        "value_count": [4],
    }

    assert result == correct_result
