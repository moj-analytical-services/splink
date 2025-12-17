from __future__ import annotations

from typing import TYPE_CHECKING, Any

from splink.internals.misc import ensure_is_iterable

if TYPE_CHECKING:
    from splink.internals.database_api import DatabaseAPI
    from splink.internals.splink_dataframe import SplinkDataFrame


def get_db_api_from_inputs(
    table_or_tables: SplinkDataFrame | Any,
) -> DatabaseAPI[Any]:
    first = next(iter(ensure_is_iterable(table_or_tables)))
    return first.db_api


def splink_dataframes_to_dict(
    table_or_tables: SplinkDataFrame | Any,
) -> dict[str, SplinkDataFrame]:
    tables = ensure_is_iterable(table_or_tables)
    return {sdf.templated_name: sdf for sdf in tables}
