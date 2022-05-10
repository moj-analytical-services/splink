import os
import boto3
from uuid import uuid4


class boto_utils:
    def __init__(
        self,
        boto3_session: boto3.session.Session,
        output_bucket: str,
    ):

        if not type(boto3_session) == boto3.session.Session:
            raise ValueError("Please enter a valid boto3 session object.")

        self.boto3_session = boto3_session
        self.bucket = output_bucket
        self.session_id = uuid4().hex[:10]

        # specify some additional prefixes
        self.s3_output_name_prefix = "splink_warehouse"
        self.s3_output = self.get_table_dir()

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
