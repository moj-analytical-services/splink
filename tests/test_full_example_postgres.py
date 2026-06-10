import pytest

pytest.importorskip("sqlalchemy")
# ruff: noqa: E402 (module level import not at top of file)

import os
from datetime import datetime

from splink.blocking_analysis import (
    chart_comparisons_from_blocking_rules,
    count_comparisons_from_blocking_rules,
)
from splink.exploratory import completeness_chart, profile_columns
from splink.internals.linker import Linker
from splink.internals.postgres.database_api import PostgresAPI

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_including


@mark_with_dialects_including("postgres")
def test_full_example_postgres(tmp_path, pg_engine, fake_1000):
    settings_dict = get_settings_dict()

    db_api = PostgresAPI(engine=pg_engine)
    df_sdf = db_api.register(fake_1000)

    linker = Linker(
        df_sdf,
        settings_dict,
    )

    count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules='l.first_name = r.first_name and l."surname" = r."surname"',  # noqa: E501
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
    )

    chart_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=[
            "l.first_name = r.first_name",
            "l.surname = r.surname",
            "l.city = r.city",
        ],
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
    )

    profile_columns(
        df_sdf,
        [
            "first_name",
            '"surname"',
            'first_name || "surname"',
            "concat(city, first_name)",
        ],
    )

    completeness_chart(df_sdf)

    linker.table_management.compute_tf_table("city")
    linker.table_management.compute_tf_table("first_name")

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.training.estimate_probability_two_random_records_match(
        ["l.email = r.email"], recall=0.3
    )

    blocking_rule = 'l.first_name = r.first_name and l."surname" = r."surname"'
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.inference.predict()

    linker.visualisations.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "test_scv_postgres.html"), True, 2
    )

    records = df_predict.as_record_dict()
    linker.visualisations.waterfall_chart(records)

    labels_sdf = df_sdf.query_sql(
        """
        WITH first_10 AS (
            SELECT * FROM {this} LIMIT 10
        )
        SELECT
            l.unique_id AS unique_id_l,
            r.unique_id AS unique_id_r,
            CASE
                WHEN l.cluster = r.cluster THEN 1.0
                ELSE 0.0
            END AS clerical_match_score
        FROM
            first_10 AS l
        CROSS JOIN
            first_10 AS r
        WHERE
            l.unique_id < r.unique_id
        """
    )
    linker.evaluation.accuracy_analysis_from_labels_table(labels_sdf.physical_name)

    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.1
    )

    linker.visualisations.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        sampling_method="by_cluster_size",
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.evaluation.unlinkables_chart(name_of_data_in_title="Testing")

    record = {
        "unique_id": 1,
        "first_name": "John",
        "surname": "Smith",
        "dob": datetime(1971, 5, 24),
        "city": "London",
        "email": "john@smith.net",
        "cluster": 10000,
    }

    linker.inference.compare_two_records(record, record)

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.misc.save_model_to_json(path)

    Linker(df_sdf, path)


@mark_with_dialects_including("postgres")
def test_postgres_use_existing_table(fake_1000, pg_engine):
    db_api = PostgresAPI(engine=pg_engine)
    table_name = "input_table_test"
    db_api.register(fake_1000, table_name)

    settings_dict = get_settings_dict()

    db_api = PostgresAPI(engine=pg_engine)
    df_sdf = db_api.register(table_name)

    linker = Linker(
        df_sdf,
        settings_dict,
    )
    linker.inference.predict()
