import os
import boto3
import datetime


def create_session_id():
    """
    Create a filepath suffix for more human readable session IDs.
    """
    return datetime.datetime.now().strftime("%Y%m%d_%H-%M-%S")


class boto_utils:
    def __init__(
        self,
        boto3_session: boto3.session.Session,
        output_bucket: str,
        output_filepath: str,
    ):

        if not type(boto3_session) == boto3.session.Session:
            raise ValueError("Please enter a valid boto3 session object.")

        self.boto3_session = boto3_session
        self.bucket = output_bucket.replace("s3://", "")

        # If the default folder is blank, name it splink_warehouse
        # add a unique session id
        if output_filepath:
            self.s3_output_name_prefix = output_filepath
            self.session_id = ""
        else:
            self.s3_output_name_prefix = "splink_warehouse"
            self.session_id = create_session_id()

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
