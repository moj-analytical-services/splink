import os

import pandas as pd
import pytest

from splink.athena.athena_comparison_library import levenshtein_at_thresholds

from .basic_settings import get_settings_dict
from .linker_utils import _test_table_registration

skip = False
try:
    import awswrangler as wr
except ImportError:
    skip = True
    pass  # Prevent failures if awswrangler is not installed

if not skip:
    import boto3
    from dataengineeringutils3.s3 import delete_s3_folder_contents

    from splink.athena.athena_linker import AthenaLinker


settings_dict = get_settings_dict()

first_name_cc = levenshtein_at_thresholds(
    col_name="first_name",
    distance_threshold_or_thresholds=2,
    include_exact_match_level=True,
    term_frequency_adjustments=True,
    m_probability_exact_match=0.7,
    m_probability_or_probabilities_lev=0.2,
    m_probability_else=0.1,
)

# Update tf weight and u probabilities to match v2 settings
first_name_cc._comparison_dict["comparison_levels"][1]._tf_adjustment_weight = 0.6
u_probabilities_first_name = [0.1, 0.1, 0.8]
for u_prob, level in zip(
    u_probabilities_first_name,
    first_name_cc._comparison_dict["comparison_levels"][1:],
):
    level._u_probability = u_prob

# Update settings w/ our edited first_name col
settings_dict["comparisons"][0] = first_name_cc


def setup_athena_db(my_session, db_name="splink_awswrangler_test"):
    """
    =====
    Partially deprecated by the new garbage_collection function.
    =====

    Run this function if you need to create, or recreate the database(s)
    used in this test.
    """

    # If our database already exists, delete and recreate it,
    # so we ensure we have no cached data to read from.
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

    return db_name_read, db_name_write


@pytest.mark.skip(reason="AWS Connection Required")
def test_full_example_athena(tmp_path):
    """
    NOTE - we've changed this test. The dbs are now hard coded and are not
    created on a new test run. Instead, we are utilising garbage_collection
    in the AthenaLinker to clean the process before we proceed.
    """

    # creates a session at least on the platform...
    my_session = boto3.Session(region_name="eu-west-1")
    db_name_read = "splink_awswrangler_test"
    db_name_write = f"{db_name_read}2"

    linker = AthenaLinker(
        settings_dict=settings_dict,
        input_table_or_tables=f"{db_name_read}.fake_1000_from_splink_demos",
        boto3_session=my_session,
        output_bucket="alpha-splink-db-testing",
        output_database=db_name_write,
        output_filepath="test_full_example",
        garbage_collection_level=2,
    )

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
    )
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_u_using_random_sampling(max_pairs=1e6)

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

    _test_table_registration(linker)


@pytest.mark.skip(reason="AWS Connection Required")
def test_athena_garbage_collection():
    # creates a session at least on the platform...
    my_session = boto3.Session(region_name="eu-west-1")
    db_name_read = "splink_awswrangler_test"
    db_name_write = f"{db_name_read}2"

    # No cleaning...
    AthenaLinker(
        settings_dict=settings_dict,
        input_table_or_tables=f"{db_name_read}.fake_1000_from_splink_demos",
        boto3_session=my_session,
        output_bucket="alpha-splink-db-testing",
        output_database=db_name_write,
        garbage_collection_level=0,
        output_filepath="test_full_example",
    )

    # Check everything gets cleaned up when initialising the linker
    tables = wr.catalog.get_tables(
        database="splink_awswrangler_test2",
        name_prefix="__splink__df_predict",  # check if the predict table exists...
        boto3_session=my_session,
    )
    assert sum(1 for _ in tables) > 0

    # Check all files are also deleted (as gc = True)
    files = wr.s3.list_objects(
        path="s3://alpha-splink-db-testing/test_full_example/",
        boto3_session=my_session,
        ignore_empty=True,
    )
    assert len(files) > 0

    # Perform cleaning
    AthenaLinker(
        settings_dict=settings_dict,
        input_table_or_tables=f"{db_name_read}.fake_1000_from_splink_demos",
        boto3_session=my_session,
        output_bucket="alpha-splink-db-testing",
        output_database=db_name_write,
        garbage_collection_level=2,
        output_filepath="test_full_example",
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
    db_name_read = "splink_awswrangler_test"
    db_name_write = f"{db_name_read}2"

    linker = AthenaLinker(
        input_table_or_tables=df,
        settings_dict=settings_dict,
        boto3_session=my_session,
        output_bucket="alpha-splink-db-testing",
        output_database=db_name_write,
        garbage_collection_level=2,
        output_filepath="test_full_example",
    )

    linker.predict()


@pytest.mark.skip(reason="AWS Connection Required")
def test_athena_link_only():
    import pandas as pd

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # creates a session at least on the platform...
    my_session = boto3.Session(region_name="eu-west-1")
    settings_dict["link_type"] = "link_and_dedupe"
    db_name_read = "splink_awswrangler_test"
    db_name_write = f"{db_name_read}2"

    linker = AthenaLinker(
        input_table_or_tables=[df, df],
        settings_dict=settings_dict,
        boto3_session=my_session,
        output_bucket="alpha-splink-db-testing",
        output_database=db_name_write,
        garbage_collection_level=2,
        output_filepath="test_full_example",
    )

    df_predict = linker.predict()
    df_predict.as_pandas_dataframe()
