from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from splink.internals.input_column import InputColumn

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from duckdb import DuckDBPyRelation

    from splink.internals.database_api import DatabaseAPI


class SplinkDataFrame(ABC):
    """Abstraction over dataframe to handle basic operations like retrieving data and
    retrieving column names, which need different implementations depending on whether
    it's a spark dataframe, sqlite table etc.
    Uses methods like `as_pandas_dataframe()` and `as_record_dict()` to retrieve data
    """

    def __init__(
        self,
        templated_name: str,
        physical_name: str,
        db_api: DatabaseAPI[Any],
        metadata: dict[str, Any] = None,
    ):
        self.templated_name = templated_name
        self.physical_name = physical_name
        self.db_api = db_api
        self._target_schema = "splink"
        self.created_by_splink = False
        self.sql_used_to_create: str = ""
        self.metadata = metadata or {}

    @property
    @abstractmethod
    def columns(self) -> list[InputColumn]:
        pass

    @property
    def columns_escaped(self):
        cols = self.columns
        return [c.name for c in cols]

    @abstractmethod
    def validate(self):
        pass

    @property
    def physical_and_template_names_equal(self):
        return self.templated_name == self.physical_name

    def _check_drop_table_created_by_splink(self, force_non_splink_table=False):
        if not self.created_by_splink:
            if not force_non_splink_table:
                raise ValueError(
                    f"You've asked to drop table {self.physical_name} from your "
                    "database which is not a table created by Splink.  If you really "
                    "want to drop this table, you can do so by setting "
                    "force_non_splink_table=True"
                )
        logger.debug(
            f"Dropping table with templated name {self.templated_name} and "
            f"physical name {self.physical_name}"
        )

    def _drop_table_from_database(self, force_non_splink_table=False):
        raise NotImplementedError(
            "_drop_table_from_database from database not " "implemented for this linker"
        )

    def drop_table_from_database_and_remove_from_cache(
        self, force_non_splink_table: bool = False
    ) -> None:
        """Drops the table from the underlying database, and removes it
        from the (linker) cache.

        By default this will fail if the table is not one created by Splink,
        but this check can be overriden

        Examples:
            ```py
            df_predict = linker.inference.predict()
            df_predict.drop_table_from_database_and_remove_from_cache()
            # predictions table no longer in the database / cache
            ```
        Args:
            force_non_splink_table (bool, optional): If True, skip check if the
                table was created by Splink and always drop regardless. If False,
                only drop if table was created by Splink. Defaults to False.

        """
        self._drop_table_from_database(force_non_splink_table=force_non_splink_table)
        self.db_api.remove_splinkdataframe_from_cache(self)

    def as_record_dict(self, limit: Optional[int] = None) -> list[dict[str, Any]]:
        """Return the dataframe as a list of record dictionaries.

        This can be computationally expensive if the dataframe is large.

        Examples:
            ```py
            df_predict = linker.inference.predict()
            ten_edges = df_predict.as_record_dict(10)
            ```
        Args:
            limit (int, optional): If provided, return this number of rows (equivalent
            to a limit statement in SQL). Defaults to None, meaning return all rows

        Returns:
            list: a list of records, each of which is a dictionary
        """
        raise NotImplementedError("as_record_dict not implemented for this linker")

    def as_pandas_dataframe(self, limit=None):
        """Return the dataframe as a pandas dataframe.

        This can be computationally expensive if the dataframe is large.

        Args:
            limit (int, optional): If provided, return this number of rows (equivalent
                to a limit statement in SQL). Defaults to None, meaning return all rows

        Examples:
            ```py
            df_predict = linker.inference.predict()
            df_ten_edges = df_predict.as_pandas_dataframe(10)
            ```
        Returns:
            pandas.DataFrame: pandas Dataframe
        """
        import pandas as pd

        return pd.DataFrame(self.as_record_dict(limit=limit))

    def as_duckdbpyrelation(self, limit: Optional[int] = None) -> DuckDBPyRelation:
        """Return the dataframe as a duckdbpyrelation.  Only available when using the
        DuckDB backend.

        Args:
            limit (int, optional): If provided, return this number of rows (equivalent
                to a limit statement in SQL). Defaults to None, meaning return all rows

        Returns:
            duckdb.DuckDBPyRelation: A DuckDBPyRelation object
        """
        raise NotImplementedError(
            "This method is only available when using the DuckDB backend"
        )

    # Spark not guaranteed to be available so return type is not imported
    def as_spark_dataframe(self) -> "SparkDataFrame":  # type: ignore # noqa: F821
        """Return the dataframe as a spark dataframe.  Only available when using the
        Spark backend.

        Returns:
            spark.DataFrame: A Spark DataFrame
        """
        raise NotImplementedError(
            "This method is only available when using the Spark backend"
        )

    def to_parquet(self, filepath, overwrite=False):
        """Save the dataframe in parquet format.

        Examples:
            ```py
            df_predict = linker.inference.predict()
            df_predict.to_parquet("model_predictions.parquet", overwrite=True)
            ```
        Args:
            filepath (str): Filepath where csv will be saved.
            overwrite (bool, optional): If True, overwrites file if it already exists.
                Default is False.
        """
        raise NotImplementedError("`to_parquet` not implemented for this linker")

    def to_csv(self, filepath, overwrite=False):
        """Save the dataframe in csv format.

        Examples:
            ```py
            df_predict = linker.inference.predict()
            df_predict.to_csv("model_predictions.csv", overwrite=True)
            ```
        Args:
            filepath (str): Filepath where csv will be saved.
            overwrite (bool, optional): If True, overwrites file if it already exists.
                Default is False.
        """
        raise NotImplementedError("`to_csv` not implemented for this linker")

    def check_file_exists(self, filepath):
        p = Path(filepath)
        if p.exists():
            raise FileExistsError(
                "The filepath you've supplied already exists. Please use "
                "either `overwrite = True` or manually move or delete the "
                "existing file."
            )

    def _repr_pretty_(self, p, cycle):
        msg = (
            f"Splink DataFrame representing table: `{self.physical_name}`\n"
            "\nTo retrieve records, call one of the `as_x()` methods e.g."
            "`.as_pandas_dataframe(limit=5)`\n"
            "or query the table using SQL with `linker.misc.query_sql(sql)`\n"
            "referring to the table with {this_df.physical_name}.\n"
        )
        p.text(msg)
