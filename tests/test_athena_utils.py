import pytest

pytest.importorskip("awswrangler")
# ruff: noqa: E402 (module level import not at top of file)

from unittest.mock import patch

from splink.internals.athena.athena_helpers.athena_utils import (
    _verify_athena_inputs,
)
from splink.internals.exceptions import InvalidAWSBucketOrDatabase

DATABASE = "my_database"
BUCKET = "my_bucket"


def _glue_databases(count, target_name=None, target_index=None):
    # Mimics the dicts yielded by wr.catalog.get_databases(), which walks
    # the full Glue paginator (as opposed to wr.catalog.databases(), which
    # truncates to its `limit` arg, 100 by default).
    databases = [{"Name": f"other_db_{i}", "Description": ""} for i in range(count)]
    if target_name is not None:
        databases[target_index] = {"Name": target_name, "Description": ""}
    return databases


@patch("awswrangler.s3.list_buckets", return_value=[BUCKET])
@patch("awswrangler.catalog.get_databases")
def test_verify_athena_inputs_small_database_list_unchanged(
    mock_get_databases, mock_list_buckets
):
    # Baseline case, well under the old wr.catalog.databases() default of
    # limit=100: behaviour should be unchanged.
    mock_get_databases.return_value = iter(_glue_databases(5, DATABASE, 2))

    _verify_athena_inputs(database=DATABASE, bucket=BUCKET, boto3_session=None)


@patch("awswrangler.s3.list_buckets", return_value=[BUCKET])
@patch("awswrangler.catalog.get_databases")
def test_verify_athena_inputs_finds_database_beyond_first_hundred(
    mock_get_databases, mock_list_buckets
):
    # Regression test for #3001: with more than 100 databases in the
    # catalog, a target database sitting beyond the old default `limit=100`
    # must still be recognised as existing.
    mock_get_databases.return_value = iter(_glue_databases(150, DATABASE, 120))

    _verify_athena_inputs(database=DATABASE, bucket=BUCKET, boto3_session=None)


@patch("awswrangler.s3.list_buckets", return_value=[BUCKET])
@patch("awswrangler.catalog.get_databases")
def test_verify_athena_inputs_missing_database_still_raises(
    mock_get_databases, mock_list_buckets
):
    # A database that genuinely does not exist anywhere in the (paginated)
    # catalog must NOT be silently accepted.
    mock_get_databases.return_value = iter(_glue_databases(150))

    with pytest.raises(InvalidAWSBucketOrDatabase, match=DATABASE):
        _verify_athena_inputs(database=DATABASE, bucket=BUCKET, boto3_session=None)


@patch("awswrangler.s3.list_buckets", return_value=[])
@patch("awswrangler.catalog.get_databases")
def test_verify_athena_inputs_missing_bucket_still_raises(
    mock_get_databases, mock_list_buckets
):
    # Secondary path, untouched by this fix: the bucket check must keep
    # raising when the bucket is absent.
    mock_get_databases.return_value = iter(_glue_databases(5, DATABASE, 2))

    with pytest.raises(InvalidAWSBucketOrDatabase, match=BUCKET):
        _verify_athena_inputs(database=DATABASE, bucket=BUCKET, boto3_session=None)
