import awswrangler as wr

from splink.exceptions import InvalidAWSBucketOrDatabase
from splink.misc import ensure_is_list
from splink.splink_dataframe import SplinkDataFrame


def athena_warning_text(database_bucket_txt, do_does_grammar):
    return (
        f"\nThe supplied {database_bucket_txt} that you have requested to write to "
        f"{do_does_grammar[0]} not currently exist. \n \nCreate "
        f"{do_does_grammar[1]} either directly from within AWS, or by using "
        "'awswrangler.athena.create_athena_bucket' for buckets or "
        "'awswrangler.catalog.create_database' for databases using the "
        "awswrangler API."
    )


def _verify_athena_inputs(database, bucket, boto3_session):
    errors = []

    if (
        database
        not in wr.catalog.databases(limit=None, boto3_session=boto3_session).values
    ):
        errors.append(f"database '{database}'")

    if bucket not in wr.s3.list_buckets(boto3_session=boto3_session):
        errors.append(f"bucket '{bucket}'")

    if errors:
        database_bucket_txt = " and ".join(errors)
        do_does_grammar = ["does", "it"] if len(errors) == 1 else ["do", "them"]
        raise InvalidAWSBucketOrDatabase(
            athena_warning_text(database_bucket_txt, do_does_grammar)
        )


def _garbage_collection(
    database_name,
    boto3_session,
    delete_s3_folders=True,
    tables_to_exclude=[],
    name_prefix="__splink",
):
    tables_to_exclude = ensure_is_list(tables_to_exclude)
    tables_to_exclude = [
        df.physical_name if isinstance(df, SplinkDataFrame) else df
        for df in tables_to_exclude
    ]

    # This will only delete tables created within the splink process. These are
    # tables containing the specific prefix: "__splink"
    tables = wr.catalog.get_tables(
        database=database_name,
        name_prefix=name_prefix,
        boto3_session=boto3_session,
    )
    delete_metadata_loc = []
    for t in tables:
        # Don't overwrite input tables if they have been
        # given the __splink prefix.
        if t["Name"] not in tables_to_exclude:
            wr.catalog.delete_table_if_exists(
                database=t["DatabaseName"],
                table=t["Name"],
                boto3_session=boto3_session,
            )
            # Only delete the backing data if requested
            if delete_s3_folders:
                path = t["StorageDescriptor"]["Location"]
                wr.s3.delete_objects(
                    path=path,
                    use_threads=True,
                    boto3_session=boto3_session,
                )
                metadata_loc = f"{path.split('/__splink')[0]}/tables/"
                if metadata_loc not in delete_metadata_loc:
                    wr.s3.delete_objects(
                        path=metadata_loc,
                        use_threads=True,
                        boto3_session=boto3_session,
                    )
                    delete_metadata_loc.append(metadata_loc)
