import logging

from .misc import escape_columns


logger = logging.getLogger(__name__)


class SplinkDataFrame:
    """Abstraction over dataframe to handle basic operations
    like retrieving columns, which need different implementations
    depending on whether it's a spark dataframe, sqlite table etc.
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
        return escape_columns(cols)

    def validate():
        pass

    def random_sample_sql(percent):
        pass

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
        import pandas as pd

        return pd.DataFrame(self.as_record_dict(limit=limit))
