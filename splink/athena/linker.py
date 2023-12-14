from __future__ import annotations

import logging
import os
from typing import Union

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd

from ..input_column import InputColumn
from ..linker import Linker
from ..logging_messages import execute_sql_logging_message_info, log_sql
from ..misc import ensure_is_list
from ..splink_dataframe import SplinkDataFrame
from ..sql_transform import sqlglot_transform_sql
from .athena_helpers.athena_transforms import cast_concat_as_varchar
from .athena_helpers.athena_utils import (
    _garbage_collection,
    _verify_athena_inputs,
)

logger = logging.getLogger(__name__)


class AthenaDataFrame(SplinkDataFrame):
    linker: AthenaLinker

    @property
    def columns(self):
        db, tb = self.linker.get_schema_info(self.physical_name)
        d = wr.catalog.get_table_types(
            database=db,
            table=tb,
            boto3_session=self.linker.boto3_session,
        )

        cols = list(d.keys())
        return [InputColumn(c, sql_dialect="presto") for c in cols]

    def validate(self):
        pass

    def _drop_table_from_database(
        self, force_non_splink_table=False, delete_s3_data=True
    ):
        # Check folder and table set for deletion
        self._check_drop_folder_created_by_splink(force_non_splink_table)
        self._check_drop_table_created_by_splink(force_non_splink_table)

        # Delete the table from s3 and your database
        table_deleted = self.linker._drop_table_from_database_if_exists(
            self.physical_name
        )
        if delete_s3_data and table_deleted:
            self.linker._delete_table_from_s3(self.physical_name)

    def drop_table_from_database_and_remove_from_cache(
        self,
        force_non_splink_table=False,
        delete_s3_data=True,
    ):
        self._drop_table_from_database(
            force_non_splink_table=force_non_splink_table, delete_s3_data=delete_s3_data
        )
        self.linker._remove_splinkdataframe_from_cache(self)

    def _check_drop_folder_created_by_splink(self, force_non_splink_table=False):
        filepath = self.linker.s3_output
        filename = self.physical_name
        # Validate that the folder is a splink generated folder...
        files = wr.s3.list_objects(
            path=os.path.join(filepath, filename),
            boto3_session=self.linker.boto3_session,
            ignore_empty=True,
        )

        if len(files) == 0:
            if not force_non_splink_table:
                raise ValueError(
                    f"You've asked to drop data housed under the filepath "
                    f"{self.linker.s3_output} from your "
                    "s3 output bucket, which is not a folder created by "
                    "Splink. If you really want to delete this data, you "
                    "can do so by setting force_non_splink_table=True."
                )

        # validate that the ctas_query_info is for the given table
        # we're interacting with
        if (
            self.linker.ctas_query_info[self.physical_name]["ctas_table"]
            != self.physical_name
        ):
            raise ValueError(
                f"The recorded metadata for {self.physical_name} that you're "
                "attempting to delete does not match the recorded metadata on s3. "
                "To prevent any tables becoming corrupted on s3, this run will be "
                "terminated. Please retry the link/dedupe job and report the issue "
                "if this error persists."
            )

    def as_pandas_dataframe(self, limit=None):
        sql = f"""
        select *
        from {self.physical_name}
        """
        if limit:
            sql += f" limit {limit}"

        out_df = wr.athena.read_sql_query(
            sql=sql,
            database=self.linker.output_schema,
            s3_output=self.linker.s3_output,
            keep_files=False,
            ctas_approach=True,
            use_threads=True,
            boto3_session=self.linker.boto3_session,
        )
        return out_df

    def as_record_dict(self, limit=None):
        out_df = self.as_pandas_dataframe(limit)
        out_df = out_df.fillna(np.nan).replace([np.nan], [None])
        return out_df.to_dict(orient="records")


class AthenaLinker(Linker):
    def __init__(
        self,
        input_table_or_tables,
        boto3_session: boto3.session.Session,
        output_database: str,
        output_bucket: str,
        settings_dict: dict | str = None,
        input_table_aliases: str | list = None,
        set_up_basic_logging=True,
        output_filepath: str = "",
        validate_settings: bool = True,
    ):
        """An athena backend for our main linker class. This funnels our generated SQL
        through athena using awswrangler.
        See linker.py for more information on the main linker class.
        Attributes:
            input_table_or_tables (Union[str, list]): Input data into the linkage model.
                Either a single string (the name of a table in a database) for
                deduplication jobs, or a list of strings  (the name of tables in a
                database) for link_only or link_and_dedupe.
            boto3_session (boto3.session.Session): A working boto3 session, which
                should contain user credentials and region information.
            output_database (str): The name of the database you wish to export the
                results of the link job to. This should be created prior to performing
                your link.
            output_bucket (str): The name of the bucket and the filepath you wish to
                store your outputs in on aws. The bucket should be created prior to
                performing your link.
            settings_dict (dict | Path, optional): A Splink settings dictionary, or
                 a path to a json defining a settingss dictionary or pre-trained model.
                  If not provided when the object is created, can later be added using
                `linker.load_settings()` or `linker.load_model()` Defaults to None.
            input_table_aliases: Aliases/custom names for your input tables, if
                a pandas df or a list of dfs are used as inputs. None by default, which
                saves your tables under a custom name: '__splink__input_table_{n}';
                where n is the list index.
            set_up_basic_logging (bool, optional): If true, sets ups up basic logging
                so that Splink sends messages at INFO level to stdout. Defaults to True.
            output_filepath (str, optional): Inside of your selected output bucket,
                where to write output files to.
                Defaults to "splink_warehouse/{unique_id}".
            validate_settings (bool, optional): When True, check your settings
                dictionary for any potential errors that may cause splink to fail.
        Examples:
            ```py
            # Creating a database in athena and writing to it
            import awswrangler as wr
            wr.catalog.create_database("splink_awswrangler_test", exist_ok=True)
            >>>
            from splink.athena.linker import AthenaLinker
            import boto3
            # Create a session - please see the boto3 documentation for more info
            my_session = boto3.Session(region_name="eu-west-1")
            >>>
            linker = AthenaLinker(
                settings_dict=settings_dict,
                input_table_or_tables="synthetic_data_all",
                boto3_session=my_session,
                output_bucket="alpha-splink-db-testing",
                output_database="splink_awswrangler_test",
            )
            ```
            ```py
            # Creating a secondary database and use data on and existing db
            import awswrangler as wr
            wr.catalog.create_database("splink_awswrangler_test2", exist_ok=True)
            >>>
            from splink.athena.linker import AthenaLinker
            import boto3
            my_session = boto3.Session(region_name="eu-west-1")
            >>>
            # To read and write from separate databases, specify your secondary
            # database as the output and enter your primary database as a schema
            # for your input table(s)
            linker = AthenaLinker(
                settings_dict=settings_dict,
                input_table_or_tables="splink_awswrangler_test.synthetic_data_all",
                boto3_session=my_session,
                output_bucket="alpha-splink-db-testing",
                output_database="splink_awswrangler_test2",
            )
            ```
        """

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

        # This query info dictionary is used to circumvent the need to run
        # `wr.catalog.get_table_location` every time we want to delete
        # the backing data from s3.
        self.ctas_query_info = {}

        # If user has provided pandas dataframes, need to register
        # them with the database, using user-provided aliases
        # if provided or a created alias if not
        input_tables = ensure_is_list(input_table_or_tables)

        input_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )
        accepted_df_dtypes = pd.DataFrame

        # Run a quick check against our inputs to check if they
        # exist in the database
        for table in input_tables:
            if not isinstance(table, accepted_df_dtypes):
                db, tb = self.get_schema_info(table)
                self.check_table_exists(db, tb)

        super().__init__(
            input_tables,
            settings_dict,
            accepted_df_dtypes,
            set_up_basic_logging,
            input_table_aliases=input_aliases,
            validate_settings=validate_settings,
        )

    def _table_to_splink_dataframe(self, templated_name, physical_name):
        return AthenaDataFrame(templated_name, physical_name, self)

    def change_output_filepath(self, new_filepath):
        self.output_filepath = new_filepath

    def get_schema_info(self, input_table):
        t = input_table.split(".")
        return t if len(t) > 1 else [self.output_schema, input_table]

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

    def check_table_exists(self, db, tb):
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

    def register_data_on_s3(self, table, alias):
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

    def _execute_sql_against_backend(self, sql, templated_name, physical_name):
        self._delete_table_from_database(physical_name)
        sql = sqlglot_transform_sql(sql, cast_concat_as_varchar, dialect="presto")
        sql = sql.replace("FLOAT", "double").replace("float", "double")

        logger.debug(execute_sql_logging_message_info(templated_name, physical_name))
        logger.log(5, log_sql(sql))

        # create our table on athena and extract the metadata information
        query_metadata = self.create_table(sql, physical_name=physical_name)
        # append our metadata locations
        query_metadata = self._extract_ctas_metadata(query_metadata)
        self.ctas_query_info.update({physical_name: query_metadata})

        output_obj = self._table_to_splink_dataframe(templated_name, physical_name)
        return output_obj

    def register_table(self, input, table_name, overwrite=False):
        # If the user has provided a table name, return it as a SplinkDataframe
        if isinstance(input, str):
            return self._table_to_splink_dataframe(table_name, input)

        # Check if table name is already in use
        exists = self._table_exists_in_database(table_name)
        if exists:
            if not overwrite:
                raise ValueError(
                    f"Table '{table_name}' already exists in database. "
                    "Please use the 'overwrite' argument if you wish to overwrite"
                )
            else:
                self._delete_table_from_database(table_name)

        self._table_registration(input, table_name)
        return self._table_to_splink_dataframe(table_name, table_name)

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Errors if an invalid data type is passed
        self.register_data_on_s3(input, table_name)

    def _random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        return f" TABLESAMPLE BERNOULLI ({percent})"

    @property
    def _infinity_expression(self):
        return "infinity()"

    def _table_exists_in_database(self, table_name):
        return wr.catalog.does_table_exist(
            database=self.output_schema,
            table=table_name,
            boto3_session=self.boto3_session,
        )

    def create_table(self, sql, physical_name):
        database = self.output_schema
        ctas_metadata = wr.athena.create_ctas_table(
            sql=sql,
            database=database,
            ctas_table=physical_name,
            storage_format="parquet",
            write_compression="snappy",
            boto3_session=self.boto3_session,
            s3_output=self.s3_output,
            wait=True,
        )
        return ctas_metadata

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

    def _delete_table_from_database(self, name):
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

    def _extract_ctas_metadata(self, ctas_metadata):
        query_meta = ctas_metadata.pop("ctas_query_metadata")
        out_locs = {
            "output_location": query_meta.output_location,
            "manifest_location": query_meta.manifest_location,
        }
        ctas_metadata.update(out_locs)
        return ctas_metadata

    def drop_all_tables_created_by_splink(
        self,
        delete_s3_folders=True,
        tables_to_exclude: list[Union[SplinkDataFrame, str]] = [],
    ):
        """Run a cleanup process for the tables created by splink and
        currently contained in your output database.
        Only those tables currently contained within your database
        will be permanently deleted. Anything existing on s3 that
        isn't connected to your database will not be removed.
        Attributes:
            delete_s3_folders (bool, optional): Whether to delete the
                backing data contained on s3. If False, the tables created
                by splink will be removed from your database, but the parquet
                outputs will remain on s3. Defaults to True.
            tables_to_exclude (list[SplinkDataFrame | str], optional): A list
                of input tables you wish to add to an ignore list. These
                will not be removed during garbage collection.
        """
        # Run cleanup on the cache before checking the db
        self.drop_tables_in_current_splink_run(
            delete_s3_folders,
            tables_to_exclude,
        )
        _garbage_collection(
            self.output_schema,
            self.boto3_session,
            delete_s3_folders,
            tables_to_exclude,
        )

    def drop_splink_tables_from_database(
        self,
        database_name: str,
        delete_s3_folders: bool = True,
        tables_to_exclude: list[Union[SplinkDataFrame, str]] = [],
    ):
        """Run a cleanup process for the tables created by splink
        in a specified database.
        Only those tables currently contained within your database
        will be permanently deleted. Anything existing on s3 that
        isn't connected to your database will not be removed.
        Attributes:
            database_name (str): The name of the database to delete splink tables from.
            delete_s3_folders (bool, optional): Whether to delete the
                backing data contained on s3. If False, the tables created
                by splink will be removed from your database, but the parquet
                outputs will remain on s3. Defaults to True.
            tables_to_exclude (list[SplinkDataFrame | str], optional): A list
                of input tables you wish to add to an ignore list. These
                will not be removed during garbage collection.
        """
        _garbage_collection(
            database_name,
            self.boto3_session,
            delete_s3_folders,
            tables_to_exclude,
        )

    def drop_tables_in_current_splink_run(
        self,
        delete_s3_folders: bool = True,
        tables_to_exclude: list[Union[SplinkDataFrame, str]] = [],
    ):
        """Run a cleanup process for the tables created
        by the current splink linker.
        This leaves tables from previous runs untouched.
        Only those tables currently contained within your database
        will be permanently deleted. Anything existing on s3 that
        isn't connected to your database will not be removed.
        Attributes:
            delete_s3_folders (bool, optional): Whether to delete the
                backing data contained on s3. If False, the tables created
                by splink will be removed from your database, but the parquet
                outputs will remain on s3. Defaults to True.
            tables_to_exclude (list[SplinkDataFrame | str], optional): A list
                of input tables you wish to add to an ignore list. These
                will not be removed during garbage collection.
        """

        tables_to_exclude = ensure_is_list(tables_to_exclude)
        tables_to_exclude = {
            df.physical_name if isinstance(df, SplinkDataFrame) else df
            for df in tables_to_exclude
        }

        # Exclude tables that the user doesn't want to delete
        cached_tables = self._intermediate_table_cache

        # Loop through our cached tables and delete all those not in our exclusion
        # list.
        for splink_df in list(cached_tables.values()):
            if (splink_df.physical_name not in tables_to_exclude) and (
                splink_df.templated_name not in tables_to_exclude
            ):
                splink_df.drop_table_from_database_and_remove_from_cache(
                    force_non_splink_table=False, delete_s3_data=delete_s3_folders
                )
            # As our cache contains duplicate term frequency tables and AWSwrangler
            # run deletions asynchronously, add any previously seen tables to the
            # list of tables to exclude from deletion.
            # This prevents attempts to delete a table that has already been purged.
            tables_to_exclude.add(splink_df.physical_name)
