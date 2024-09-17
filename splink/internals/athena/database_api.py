from __future__ import annotations

import json
import logging
import os
from typing import Any

import awswrangler as wr
import boto3
import pandas as pd

from ..database_api import DatabaseAPI
from ..dialects import AthenaDialect
from ..sql_transform import sqlglot_transform_sql
from .athena_helpers.athena_transforms import cast_concat_as_varchar
from .athena_helpers.athena_utils import (
    _verify_athena_inputs,
)
from .dataframe import AthenaDataFrame

logger = logging.getLogger(__name__)


# Dict because there's not really a 'tablish' type in Athena
class AthenaAPI(DatabaseAPI[dict[str, Any]]):
    sql_dialect = AthenaDialect()

    def __init__(
        self,
        boto3_session: boto3.session.Session,
        output_database: str,
        output_bucket: str,
        output_filepath: str = None,
    ):
        super().__init__()
        if not type(boto3_session) == boto3.session.Session:
            raise ValueError("Please enter a valid boto3 session object.")

        self._sql_dialect_ = "presto"

        _verify_athena_inputs(output_database, output_bucket, boto3_session)
        self.boto3_session = boto3_session
        self.output_schema = output_database
        self.output_bucket = output_bucket

        # If the default folder is blank, name it `splink_warehouse`
        if output_filepath:
            self.output_filepath = output_filepath
        else:
            self.output_filepath = "splink_warehouse"

        self.ctas_query_info: dict[str, Any] = {}

        # TODO: How to run this check without the input_tables?
        # Run a quick check against our inputs to check if they
        # exist in the database
        # for table in input_tables:
        #     if not isinstance(table, self.accepted_df_dtypes):
        #         db, tb = self.get_schema_info(table)
        #         self._check_table_exists(db, tb)

    @property
    def s3_output(self):
        out_path = os.path.join(
            "s3://",
            self.output_bucket,
            self.output_filepath,
            self._cache_uid,  # added in the super() step
        )
        if out_path[-1] != "/":
            out_path += "/"

        return out_path

    # TODO: Should output_filepath use getters and setters?
    def change_output_filepath(self, new_filepath):
        self.output_filepath = new_filepath

    def get_schema_info(self, input_table: str) -> list[str]:
        t = input_table.split(".")
        return t if len(t) > 1 else [self.output_schema, input_table]

    def _check_table_exists(self, db: str, tb: str) -> None:
        # A quick function to check if a table exists
        # and spit out a warning if it is not found.
        table_exists = wr.catalog.does_table_exist(
            database=db,
            table=tb,
            boto3_session=self.boto3_session,
        )
        if not table_exists:
            raise wr.exceptions.InvalidTable(
                f"Table '{tb}' was not found within your selected "
                f"database '{db}'. Please verify your input table "
                "exists."
            )

    def _drop_table_from_database_if_exists(self, table):
        return wr.catalog.delete_table_if_exists(
            database=self.output_schema, table=table, boto3_session=self.boto3_session
        )

    def _delete_table_from_s3(self, physical_name):
        path = f"{self.s3_output}{physical_name}/"
        # delete our folder
        wr.s3.delete_objects(
            path=path,
            use_threads=True,
            boto3_session=self.boto3_session,
        )

        metadata = self.ctas_query_info[physical_name]
        if "output_location" in metadata:
            metadata_urls = [
                # metadata output location
                f"{metadata['output_location']}.metadata",
                # manifest location
                metadata["manifest_location"],
            ]
            # delete our metadata
            wr.s3.delete_objects(
                path=metadata_urls,
                use_threads=True,
                boto3_session=self.boto3_session,
            )

        self.ctas_query_info.pop(physical_name)

    def delete_table_from_database(self, name):
        if name in self.ctas_query_info:
            # Use ctas metadata to delete backing data
            self._delete_table_from_s3(name)
        else:
            # If the location we want to write to already exists,
            # clean this before continuing.
            loc = f"{self.s3_output}{name}"
            folder_exists = wr.s3.list_directories(
                loc,
                boto3_session=self.boto3_session,
            )
            if folder_exists:
                # This will only delete objects we are required to delete
                wr.s3.delete_objects(
                    path=loc,
                    use_threads=True,
                    boto3_session=self.boto3_session,
                )

        self._drop_table_from_database_if_exists(name)

    def _register_data_on_s3(self, table, alias):
        out_loc = f"{self.s3_output}{alias}"

        wr.s3.to_parquet(
            df=table,
            path=out_loc,
            dataset=True,
            mode="overwrite",
            database=self.output_schema,
            table=alias,
            boto3_session=self.boto3_session,
            compression="snappy",
            use_threads=True,
        )
        # Construct the ctas metadata that we require
        ctas_metadata = {
            "ctas_database": self.output_schema,
            "ctas_table": alias,
        }
        self.ctas_query_info.update({alias: ctas_metadata})

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Errors if an invalid data type is passed
        self._register_data_on_s3(input, table_name)

    def table_to_splink_dataframe(self, templated_name, physical_name):
        return AthenaDataFrame(templated_name, physical_name, self)

    def _create_table(self, sql, physical_name):
        ctas_metadata = wr.athena.create_ctas_table(
            sql=sql,
            database=self.output_schema,
            ctas_table=physical_name,
            storage_format="parquet",
            write_compression="snappy",
            boto3_session=self.boto3_session,
            s3_output=self.s3_output,
            wait=True,
        )
        return ctas_metadata

    def table_exists_in_database(self, table_name):
        return wr.catalog.does_table_exist(
            database=self.output_schema,
            table=table_name,
            boto3_session=self.boto3_session,
        )

    def _extract_ctas_metadata(self, ctas_metadata):
        query_meta = ctas_metadata.pop("ctas_query_metadata")
        out_locs = {
            "output_location": query_meta.output_location,
            "manifest_location": query_meta.manifest_location,
        }
        ctas_metadata.update(out_locs)
        return ctas_metadata

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:
        self.delete_table_from_database(physical_name)
        # This is a hack because execute_sql_against_backend
        # needs the physical name but the _execute_sql_against_backend
        # method just takes a  string
        return json.dumps(
            {
                "physical_name": physical_name,
                "sql": sql,
            }
        )

    def _execute_sql_against_backend(self, sql):
        sql_dict = json.loads(sql)
        physical_name = sql_dict["physical_name"]
        sql_query = sql_dict["sql"]
        sql_query = sqlglot_transform_sql(
            sql_query, cast_concat_as_varchar, sqlglot_dialect="presto"
        )
        sql_query = sql_query.replace("FLOAT", "double").replace("float", "double")

        # create our table on athena and extract the metadata information
        query_metadata = self._create_table(sql_query, physical_name=physical_name)
        # append our metadata locations
        query_metadata = self._extract_ctas_metadata(query_metadata)
        self.ctas_query_info.update({physical_name: query_metadata})

        return query_metadata

    @property
    def accepted_df_dtypes(self):
        accepted_df_dtypes = [pd.DataFrame]
        try:
            # If pyarrow is installed, add to the accepted list
            import pyarrow as pa

            accepted_df_dtypes.append(pa.lib.Table)
        except ImportError:
            pass
        return accepted_df_dtypes
