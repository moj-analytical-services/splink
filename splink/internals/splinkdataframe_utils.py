from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Sequence

from splink.internals.splink_dataframe import SplinkDataFrame

if TYPE_CHECKING:
    from splink.internals.database_api import DatabaseAPI
    from splink.internals.splink_dataframe import SplinkDataFrame


def get_db_api_from_inputs(
    table_or_tables: SplinkDataFrame | Sequence[SplinkDataFrame],
) -> DatabaseAPI[Any]:
    tables: Iterable[SplinkDataFrame]
    if isinstance(table_or_tables, SplinkDataFrame):
        tables = [table_or_tables]
    else:
        tables = table_or_tables
    first = next(iter(tables))
    return first.db_api


def splink_dataframes_to_dict(
    table_or_tables: SplinkDataFrame | Sequence[SplinkDataFrame],
) -> dict[str, SplinkDataFrame]:
    tables: Iterable[SplinkDataFrame]
    if isinstance(table_or_tables, SplinkDataFrame):
        tables = [table_or_tables]
    else:
        tables = table_or_tables

    return {sdf.templated_name: sdf for sdf in tables}
