import pandas as pd
import pytest
import os

from basic_settings import get_settings_dict

skip = False
try:
    import awswrangler as wr
except ImportError:
    skip = True
    pass  # Prevent failures if awswrangler is not installed

if not skip:
    from splink.athena.athena_linker import AthenaLinker
    import boto3
    from dataengineeringutils3.s3 import delete_s3_folder_contents


def setup_athena_db(my_session, db_name="splink_awswrangler_test"):

    """
    Run this function if you need to create, or recreate the database(s)
    used in this test.
    """

    # If our database already exists, delete and recreate it,
    # so we ensure we have no cached datasets to read from.
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

    return db_name


def upload_data(db_name):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    bucket = "alpha-splink-db-testing"
    # ensure data is in a unique loc to __splink tables
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


def create_and_upload_test_data(my_session):
    db_name_read = setup_athena_db(my_session)
    db_name_write = setup_athena_db(
        my_session,
        db_name="splink_awswrangler_test2",
    )
    upload_data("splink_awswrangler_test")


@pytest.mark.skip(reason="AWS Connection Required")
def test_full_example_athena(tmp_path):

    """
    NOTE - we've changed this test. The dbs are now hard coded and are not
    created on a new test run. Instead, we are utilising garbage_collection
    in the AthenaLinker to clean the process before we proceed.
    """

    # creates a session at least on the platform...
    my_session = boto3.Session(region_name="eu-west-1")
    settings_dict = get_settings_dict()
    db_name_read = "splink_awswrangler_test"
    db_name_write = f"{db_name_read}2"

    linker = AthenaLinker(
        settings_dict=settings_dict,
        input_table_or_tables=f"{db_name_read}.fake_1000_from_splink_demos",
        boto3_session=my_session,
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

    linker.comparison_viewer_dashboard(df_predict, "test_scv_athena.html", True, 2)

    df_predict.as_pandas_dataframe()

    df_clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.1)

    linker.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        sampling_method="by_cluster_size",
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.unlinkables_chart(source_dataset="Testing")


@pytest.mark.skip(reason="AWS Connection Required")
def test_athena_garbage_collection():

    # creates a session at least on the platform...
    my_session = boto3.Session(region_name="eu-west-1")
    settings_dict = get_settings_dict()
    db_name_read = "splink_awswrangler_test"
    db_name_write = f"{db_name_read}2"

    linker = AthenaLinker(
        settings_dict=settings_dict,
        input_table_or_tables=f"{db_name_read}.fake_1000_from_splink_demos",
        boto3_session=my_session,
        output_bucket="alpha-splink-db-testing",
        output_database=db_name_write,
        garbage_collection=True,
    )

    # Check everything gets cleaned up when initialising the linker
    tables = wr.catalog.get_tables(
        database="splink_awswrangler_test2",
        name_prefix="__splink",
        boto3_session=my_session,
    )
    assert sum(1 for _ in tables) == 0

    # Check all files are also deleted (as gc = True)
    files = wr.s3.list_objects(
        path="s3://alpha-splink-db-testing/splink_warehouse",
        boto3_session=my_session,
        ignore_empty=True,
    )
    assert len(files) == 0


@pytest.mark.skip(reason="AWS Connection Required")
def test_athena_df_as_input():

    import pandas as pd

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # creates a session at least on the platform...
    my_session = boto3.Session(region_name="eu-west-1")
    settings_dict = get_settings_dict()
    db_name_read = "splink_awswrangler_test"
    db_name_write = f"{db_name_read}2"

    linker = AthenaLinker(
        input_table_or_tables=df,
        settings_dict=settings_dict,
        boto3_session=my_session,
        output_bucket="alpha-splink-db-testing",
        output_database=db_name_write,
        garbage_collection=True,
    )

    linker.predict()
