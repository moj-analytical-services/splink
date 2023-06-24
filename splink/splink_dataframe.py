from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


class SplinkDataFrame:
    """Abstraction over dataframe to handle basic operations like retrieving data and
    retrieving column names, which need different implementations depending on whether
    it's a spark dataframe, sqlite table etc.
    Uses methods like `as_pandas_dataframe()` and `as_record_dict()` to retrieve data
    """

    def __init__(self, templated_name: str, physical_name: str, linker: Linker):
        self.templated_name = templated_name
        self.physical_name = physical_name
        self.linker = linker
        self._target_schema = "splink"
        self.created_by_splink = False
        self.sql_used_to_create = None

    @property
    def columns(self):
        pass

    @property
    def columns_escaped(self):
        cols = self.columns
        return [c.name() for c in cols]

    def validate():
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
        self, force_non_splink_table=False
    ):
        self._drop_table_from_database(force_non_splink_table=force_non_splink_table)
        self.linker._remove_splinkdataframe_from_cache(self)

    def as_record_dict(self, limit=None):
        raise NotImplementedError("as_record_dict not implemented for this linker")

    def as_pandas_dataframe(self, limit=None):
        """Return the dataframe as a pandas dataframe.

        This can be computationally expensive if the dataframe is large.

        Args:
            limit (int, optional): If provided, return this number of rows (equivalent
            to a limit statement in SQL). Defaults to None, meaning return all rows

        Returns:
            pandas.DataFrame: pandas Dataframe
        """
        import pandas as pd

        return pd.DataFrame(self.as_record_dict(limit=limit))

    def _repr_pretty_(self, p, cycle):
        msg = (
            f"Table name in database: `{self.physical_name}`\n"
            "\nTo retrieve records, you can call the following methods on this object:"
            "\n`.as_record_dict(limit=5)` or "
            "`.as_pandas_dataframe(limit=5)`.\n"
            "\nYou may omit the `limit` argument to return all records."
            "\n\nThis table represents the following splink entity: "
            f"{self.templated_name}"
        )
        p.text(msg)

    def to_parquet(self, filepath, overwrite=False):
        raise NotImplementedError("`to_parquet` not implemented for this linker")

    def to_csv(self, filepath, overwrite=False):
        raise NotImplementedError("`to_csv` not implemented for this linker")

    def check_file_exists(self, filepath):
        p = Path(filepath)
        if p.exists():
            raise FileExistsError(
                "The filepath you've supplied already exists. Please use "
                "either `overwrite = True` or manually move or delete the "
                "existing file."
            )
