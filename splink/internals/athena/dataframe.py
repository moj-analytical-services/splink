from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Optional

import awswrangler as wr
import numpy as np
from pandas import DataFrame as pd_DataFrame

from ..input_column import InputColumn
from ..splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from .database_api import AthenaAPI


class AthenaDataFrame(SplinkDataFrame):
    db_api: AthenaAPI

    @property
    def columns(self) -> list[InputColumn]:
        db, tb = self.db_api.get_schema_info(self.physical_name)
        d = wr.catalog.get_table_types(
            database=db,
            table=tb,
            boto3_session=self.db_api.boto3_session,
        )
        if d is None:
            # TODO: maybe this should be an error?
            return []

        cols = list(d.keys())
        return [InputColumn(c, sqlglot_dialect_str="presto") for c in cols]

    def validate(self):
        pass

    def _drop_table_from_database(
        self, force_non_splink_table: bool = False, delete_s3_data: bool = True
    ) -> None:
        # Check folder and table set for deletion
        self._check_drop_folder_created_by_splink(force_non_splink_table)
        self._check_drop_table_created_by_splink(force_non_splink_table)

        # Delete the table from s3 and your database
        table_deleted = self.db_api._drop_table_from_database_if_exists(
            self.physical_name
        )
        if delete_s3_data and table_deleted:
            self.db_api._delete_table_from_s3(self.physical_name)

    def drop_table_from_database_and_remove_from_cache(
        self,
        force_non_splink_table: bool = False,
        delete_s3_data: bool = True,
    ) -> None:
        self._drop_table_from_database(
            force_non_splink_table=force_non_splink_table, delete_s3_data=delete_s3_data
        )
        self.db_api.remove_splinkdataframe_from_cache(self)

    def _check_drop_folder_created_by_splink(
        self, force_non_splink_table: bool = False
    ) -> None:
        filepath = self.db_api.s3_output
        filename = self.physical_name
        # Validate that the folder is a splink generated folder...
        files = wr.s3.list_objects(
            path=os.path.join(filepath, filename),
            boto3_session=self.db_api.boto3_session,
            ignore_empty=True,
        )

        if len(files) == 0:
            if not force_non_splink_table:
                raise ValueError(
                    f"You've asked to drop data housed under the filepath "
                    f"{self.db_api.s3_output} from your "
                    "s3 output bucket, which is not a folder created by "
                    "Splink. If you really want to delete this data, you "
                    "can do so by setting force_non_splink_table=True."
                )

        # validate that the ctas_query_info is for the given table
        # we're interacting with
        if (
            self.db_api.ctas_query_info[self.physical_name]["ctas_table"]
            != self.physical_name
        ):
            raise ValueError(
                f"The recorded metadata for {self.physical_name} that you're "
                "attempting to delete does not match the recorded metadata on s3. "
                "To prevent any tables becoming corrupted on s3, this run will be "
                "terminated. Please retry the link/dedupe job and report the issue "
                "if this error persists."
            )

    def as_pandas_dataframe(self, limit: Optional[int] = None) -> pd_DataFrame:
        sql = f"""
        select *
        from {self.physical_name}
        """
        if limit:
            sql += f" limit {limit}"

        out_df = wr.athena.read_sql_query(
            sql=sql,
            database=self.db_api.output_schema,
            s3_output=self.db_api.s3_output,
            keep_files=False,
            ctas_approach=True,
            use_threads=True,
            boto3_session=self.db_api.boto3_session,
        )
        return out_df

    def as_record_dict(self, limit: Optional[int] = None) -> list[Dict[str, Any]]:
        out_df = self.as_pandas_dataframe(limit)
        out_df = out_df.fillna(np.nan).replace([np.nan], [None])
        return out_df.to_dict(orient="records")
