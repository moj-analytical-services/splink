import awswrangler as wr

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
        raise Exception(athena_warning_text(database_bucket_txt, do_does_grammar))


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

    def get_table_dir(self):
        out_path = os.path.join(
            "s3://",
            self.bucket,
            self.s3_output_name_prefix,
            self.session_id,
        )
        if out_path[-1] != "/":
            out_path += "/"

        return out_path
