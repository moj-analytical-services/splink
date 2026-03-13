from typing import TYPE_CHECKING

from splink.internals.splink_dataframe import SplinkDataFrame

if TYPE_CHECKING:
    from .database_api import SnowflakeAPI


class SnowflakeDataframe(SplinkDataFrame):
    db_api: SnowflakeAPI
