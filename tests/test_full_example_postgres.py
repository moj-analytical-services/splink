import os

import pandas as pd

from splink.blocking_analysis import (
    count_comparisons_from_blocking_rule,
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)
from splink.exploratory import completeness_chart, profile_columns
from splink.internals.linker import Linker
from splink.internals.postgres.database_api import PostgresAPI

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_including
from .linker_utils import _test_table_registration, register_roc_data


@mark_with_dialects_including("postgres")
def test_full_example_postgres(tmp_path, pg_engine):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings_dict = get_settings_dict()

    db_api = PostgresAPI(engine=pg_engine)
    linker = Linker(
        df,
        settings_dict,
        db_api=db_api,
    )

    count_comparisons_from_blocking_rule(
        table_or_tables=df,
        blocking_rule='l.first_name = r.first_name and l."surname" = r."surname"',  # noqa: E501
        link_type="dedupe_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
    )

    cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
        table_or_tables=df,
        blocking_rules=[
            "l.first_name = r.first_name",
            "l.surname = r.surname",
            "l.city = r.city",
        ],
        link_type="dedupe_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
    )

    profile_columns(
        df,
        db_api,
        [
            "first_name",
            '"surname"',
            'first_name || "surname"',
            "concat(city, first_name)",
        ],
    )

    completeness_chart(df, db_api=db_api)

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

    df_e = df_predict.as_pandas_dataframe(limit=5)
    records = df_e.to_dict(orient="records")
    linker.visualisations.waterfall_chart(records)

    register_roc_data(linker)

    linker.evaluation.accuracy_analysis_from_labels_table("labels")

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

    _test_table_registration(linker)

    record = {
        "unique_id": 1,
        "first_name": "John",
        "surname": "Smith",
        "dob": "1971-05-24",
        "city": "London",
        "email": "john@smith.net",
        "cluster": 10000,
    }

    linker.inference.find_matches_to_new_records(
        [record], blocking_rules=[], match_weight_threshold=-10000
    )

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.misc.save_model_to_json(path)

    Linker(df, path, db_api=db_api)


@mark_with_dialects_including("postgres")
def test_postgres_use_existing_table(tmp_path, pg_engine):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    table_name = "input_table_test"
    df.to_sql(table_name, pg_engine)

    settings_dict = get_settings_dict()

    db_api = PostgresAPI(engine=pg_engine)
    linker = Linker(
        table_name,
        db_api=db_api,
        settings=settings_dict,
    )
    linker.inference.predict()
