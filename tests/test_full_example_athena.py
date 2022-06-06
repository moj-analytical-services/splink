import os
from splink.athena.athena_linker import AthenaLinker
import pandas as pd

from basic_settings import get_settings_dict

import boto3
from dataengineeringutils3.s3 import delete_s3_folder_contents
import awswrangler as wr


def setup_athena_db(db_name="splink_awswrangler_test"):

    # creates a session at least on the platform...
    my_session = boto3.Session(region_name="eu-west-1")

    ## DELETE + RECREATE OUR DATABASE TO WRITE TO
    # reset our db for another test run...
    if db_name in wr.catalog.databases(limit=10000).values:
        wr.catalog.delete_database(name=db_name, boto3_session=my_session)
        # clean up folder contents from s3...
        # can potentially add this as a module to our awslinker
        delete_s3_folder_contents("s3://alpha-splink-db-testing/splink_warehouse/")
        delete_s3_folder_contents("s3://alpha-splink-db-testing/my_random_folder/")
        delete_s3_folder_contents("s3://alpha-splink-db-testing/data/")

    if db_name not in wr.catalog.databases(limit=10000).values:
        import time

        time.sleep(3)
        wr.catalog.create_database(db_name, exist_ok=True)

    return my_session, db_name


def upload_data(db_name):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    bucket = "alpha-splink-db-testing"
    path = f"s3://{bucket}/data/"

    table_name = "fake_1000_from_splink_demos"
    path = f"s3://{bucket}/data/"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=db_name,
        table=table_name,
    )


@pytest.mark.skip(reason="AWS Connection Required")
def test_full_example_athena(tmp_path):

    session_read, db_name_read = setup_athena_db()
    session_write, db_name_write = setup_athena_db(db_name="splink_awswrangler_test2")
    upload_data("splink_awswrangler_test")
    settings_dict = get_settings_dict()

    # Update first name settings
    settings_dict["comparisons"][0]["comparison_levels"][2] = {
        "sql_condition": "levenshtein_distance(first_name_l, first_name_r) <= 2",
        "m_probability": 0.2,
        "u_probability": 0.1,
        "label_for_charts": "Levenstein <= 2",
    }

    linker = AthenaLinker(
        settings_dict=settings_dict,
        input_table_or_tables=f"{db_name_read}.fake_1000_from_splink_demos",
        boto3_session=session_write,
        output_bucket="alpha-splink-db-testing",
        output_database=db_name_write,
    )

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
    )
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_u_using_random_sampling(target_rows=1e6)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    df_predict.as_pandas_dataframe()

    df_clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.1)

    linker.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        [0, 4],
        os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.unlinkables_chart(source_dataset="Testing")
