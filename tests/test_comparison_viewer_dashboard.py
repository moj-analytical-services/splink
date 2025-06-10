import os

import pandas as pd

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
def test_comparison_viewer_dashboard(tmp_path):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings_dict = get_settings_dict()

    linker = Linker(
        df,
        settings=settings_dict,
        db_api=DuckDBAPI(),
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
    df = pd.DataFrame(
        {
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
    )

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

    linker = Linker(
        df,
        settings=settings_dict,
        db_api=DuckDBAPI(),
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
    result = df.as_pandas_dataframe()[["gamma_first_name", "count"]]
    result = result.value_counts().reset_index(name="value_count")

    correct_result = pd.DataFrame(
        {
            "gamma_first_name": [0, 1],
            "count": [5, 10],
            "value_count": [4, 4],
        }
    )

    pd.testing.assert_frame_equal(result, correct_result, check_dtype=False)

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
    result = df.as_pandas_dataframe()[["gamma_first_name", "count"]]
    result = result.value_counts().reset_index(name="value_count")

    correct_result = pd.DataFrame(
        {
            "gamma_first_name": [1],
            "count": [10],
            "value_count": [4],
        }
    )

    pd.testing.assert_frame_equal(result, correct_result, check_dtype=False)
