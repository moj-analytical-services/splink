import os

import pandas as pd
import pytest

from splink.postgres.linker import PostgresLinker

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_including
from .linker_utils import _test_table_registration, register_roc_data


@mark_with_dialects_including("postgres")
def test_full_example_postgres(tmp_path, pg_engine):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings_dict = get_settings_dict()

    linker = PostgresLinker(
        df,
        engine=pg_engine,
    )
    linker.load_settings(settings_dict)

    linker.count_num_comparisons_from_blocking_rule(
        'l.first_name = r.first_name and l."surname" = r."surname"'
    )
    linker.cumulative_num_comparisons_from_blocking_rules_chart(
        [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
            "l.city = r.city",
        ]
    )

    linker.profile_columns(
        [
            "first_name",
            '"surname"',
            'first_name || "surname"',
            "concat(city, first_name)",
        ]
    )
    linker.missingness_chart()
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.estimate_probability_two_random_records_match(
        ["l.email = r.email"], recall=0.3
    )
    # try missingness chart again now that concat_with_tf is precomputed
    linker.missingness_chart()

    blocking_rule = 'l.first_name = r.first_name and l."surname" = r."surname"'
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    linker.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "test_scv_postgres.html"), True, 2
    )

    df_e = df_predict.as_pandas_dataframe(limit=5)
    records = df_e.to_dict(orient="records")
    linker.waterfall_chart(records)

    register_roc_data(linker)
    linker.roc_chart_from_labels_table("labels")
    linker.accuracy_chart_from_labels_table("labels")
    linker.confusion_matrix_from_labels_table("labels")

    df_clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.1)

    linker.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        sampling_method="by_cluster_size",
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.unlinkables_chart(source_dataset="Testing")

    _test_table_registration(linker)

    record = {
        "unique_id": 1,
        "first_name": "John",
        "surname": "Smith",
        "dob": "1971-05-24",
        "city": "London",
        "email": "john@smith.net",
        "group": 10000,
    }

    linker.find_matches_to_new_records(
        [record], blocking_rules=[], match_weight_threshold=-10000
    )

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.save_settings_to_json(path)

    linker_2 = PostgresLinker(df, engine=pg_engine)
    linker_2.load_settings(path)
    linker_2.load_settings_from_json(path)


@mark_with_dialects_including("postgres")
def test_postgres_use_existing_table(tmp_path, pg_engine):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    table_name = "input_table_test"
    df.to_sql(table_name, pg_engine)

    settings_dict = get_settings_dict()

    linker = PostgresLinker(
        table_name,
        engine=pg_engine,
        settings_dict=settings_dict,
    )
    linker.predict()


@mark_with_dialects_including("postgres")
def test_error_no_connection():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    # get an error as we don't pass a connection
    with pytest.raises(ValueError):
        PostgresLinker(df)
