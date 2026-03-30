from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Sequence

from splink.internals.exceptions import SplinkException
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
        tables = list(table_or_tables)

    if not tables:
        raise SplinkException("At least one SplinkDataFrame must be provided.")

    first = tables[0]
    first_db_api = first.db_api

    for sdf in tables[1:]:
        if sdf.db_api is not first_db_api:
            raise SplinkException(
                "All input SplinkDataFrames must be registered against the same "
                "database API.\n"
                f"Table '{first.templated_name}' is registered with a "
                f"{type(first_db_api).__name__} with id='{first_db_api.id}', "
                f"but table '{sdf.templated_name}' is registered with a "
                f"{type(sdf.db_api).__name__} with id='{sdf.db_api.id}'.\n"
                "Please ensure all tables are registered using the same db_api. "
                "You can check the id of your db_api using db_api.id"
            )

    return first_db_api


def splink_dataframes_to_dict(
    table_or_tables: SplinkDataFrame | Sequence[SplinkDataFrame],
) -> dict[str, SplinkDataFrame]:
    tables: Iterable[SplinkDataFrame]
    if isinstance(table_or_tables, SplinkDataFrame):
        tables = [table_or_tables]
    else:
        tables = table_or_tables

    return {sdf.templated_name: sdf for sdf in tables}
