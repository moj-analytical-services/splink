import logging


logger = logging.getLogger(__name__)


class SplinkDataFrame:
    """Abstraction over dataframe to handle basic operations like retrieving data and
    retrieving column names, which need different implementations depending on whether
    it's a spark dataframe, sqlite table etc.

    Uses methods like `as_pandas_dataframe()` and `as_record_dict()` to retrieve data
    """

    def __init__(self, templated_name, physical_name):
        self.templated_name = templated_name
        self.physical_name = physical_name

    @property
    def columns(self):
        pass

    @property
    def columns_escaped(self):
        cols = self.columns
        return [c.name() for c in cols]

    def validate():
        pass

    def _random_sample_sql(percent):
        raise NotImplementedError("Random sample sql not implemented for this linker")

    @property
    def physical_and_template_names_equal(self):
        return self.templated_name == self.physical_name

    def _check_drop_table_created_by_splink(self, force_non_splink_table=False):

        if not self.physical_name.startswith("__splink__"):
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

    def drop_table_from_database(self, force_non_splink_table=False):
        raise NotImplementedError(
            "Drop table from database not implemented for this linker"
        )

    def as_record_dict(self, limit=None):
        pass

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

    def __repr__(self):
        return (
            f"Table name in database: `{self.physical_name}`\n"
            "\nTo retrieve records, you can call the following methods on this object:"
            "\n`.as_record_dict(limit=5)` or "
            "`.as_pandas_dataframe(limit=5)`.\n"
            "\nYou may omit the `limit` argument to return all records."
            "\n\nThis table represents the following splink entity: "
            f"{self.templated_name}"
        )
