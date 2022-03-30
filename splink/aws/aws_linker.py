import sqlglot
from splink.linker import Linker, SplinkDataFrame
import awswrangler as wr
import boto3
from splink.aws.aws_utils import boto_utils

import time

class AWSDataFrame(SplinkDataFrame):
    def __init__(self, templated_name, physical_name, aws_linker):
        super().__init__(templated_name, physical_name)
        self.aws_linker = aws_linker

    @property
    def columns(self):
        d = self.as_record_dict(1)[0]

        return list(d.keys())

    def validate(self):
        pass

    def as_record_dict(self, limit=None):
        sql = f"""
        select *
        from {self.physical_name};
        """
        out_df = wr.athena.read_sql_query(
            sql=sql, 
            database=self.aws_linker.database_name,
            s3_output=self.aws_linker.boto_utils.s3_output,
            keep_files = False,
        )
        return out_df.to_dict(orient="records")


class AWSLinker(Linker):
    def __init__(self, settings_dict: dict,
                 boto3_session: boto3.session.Session, 
                 output_bucket: str,
                 database_name: str,
                 input_tables={},
                ):
        self.boto3_session = boto3_session
        self.database_name = database_name
        self.boto_utils = boto_utils(boto3_session, output_bucket)
        super().__init__(settings_dict, input_tables)

    def _df_as_obj(self, templated_name, physical_name):
        return AWSDataFrame(templated_name, physical_name, self)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):
        
        # Deletes the table in the db, but not the object on s3,
        # which needs to be manually deleted at present
        # We can adjust this to be manually cleaned, but it presents
        # a potential area for concern for users (actively deleting from aws accounts)
        # might represent a bit of a security concern
        self.delete_table_from_database(physical_name)
        
#         if transpile:
#             sql = sqlglot.transpile(sql, read="spark", write="presto")[0]
#         print(f"===== Creating {physical_name} =====")
#         self.create_table(sql, physical_name=physical_name)

#         output_obj = self._df_as_obj(templated_name, physical_name)
#         return output_obj

        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="presto")[0]
        print(f"===== Creating {physical_name} =====")
        t = time.time()
        self.create_table(sql, physical_name=physical_name)
        print(f"Creation took {time.time()-t} seconds")
        
        output_obj = self._df_as_obj(templated_name, physical_name)
        return output_obj

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""

        sample_size = int(sample_size)

        return (
            "where unique_id IN (SELECT unique_id FROM __splink__df_concat_with_tf"
            f" ORDER BY RANDOM() LIMIT {sample_size})"
        )

    def table_exists_in_database(self, table_name):
        rec = wr.catalog.does_table_exist(
            database=self.database_name, 
            table=table_name,
            boto3_session=self.boto3_session
        )
        if not rec:
            return False
        else:
            return True

    def create_table(self, sql, physical_name):
        database = self.database_name
        wr.athena.create_ctas_table(
            sql=sql,
            database=database, # adjust this later for added flexibility
            ctas_table=physical_name,
            ctas_database=database, # change to edit where we write (defaults to your current db)
            storage_format="parquet",
            write_compression="snappy",
            boto3_session=self.boto3_session,
            s3_output=self.boto_utils.s3_output,
            wait=True,
        )

    def delete_table_from_database(self, table):
        wr.catalog.delete_table_if_exists(
            database=self.database_name,
            table=table,
            boto3_session=self.boto3_session
        )
