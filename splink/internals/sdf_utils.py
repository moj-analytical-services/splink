"""Utility functions for working with SplinkDataFrame objects."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Dict, List, Union

if TYPE_CHECKING:
    from splink.internals.database_api import DatabaseAPI
    from splink.internals.splink_dataframe import SplinkDataFrame


def get_db_api_from_inputs(
    table_or_tables: Union["SplinkDataFrame", Sequence["SplinkDataFrame"]],
) -> "DatabaseAPI[Any]":
    """Extract db_api from SplinkDataFrame input(s).

    Args:
        table_or_tables: A single SplinkDataFrame or a list of SplinkDataFrames

    Returns:
        The DatabaseAPI instance associated with the input(s)

    Raises:
        TypeError: If input is not a SplinkDataFrame
        ValueError: If multiple inputs have different db_api instances
    """
    from splink.internals.splink_dataframe import SplinkDataFrame

    # Normalize to list
    if isinstance(table_or_tables, SplinkDataFrame):
        tables = [table_or_tables]
    elif isinstance(table_or_tables, (list, tuple)):
        tables = list(table_or_tables)
    else:
        raise TypeError(
            f"Expected SplinkDataFrame or list of SplinkDataFrames, "
            f"got {type(table_or_tables).__name__}. "
            f"You must register your data using db_api.register() before passing "
            f"it to this function."
        )

    if len(tables) == 0:
        raise ValueError("At least one table must be provided")

    # Validate all inputs are SplinkDataFrames
    for i, table in enumerate(tables):
        if not isinstance(table, SplinkDataFrame):
            raise TypeError(
                f"Expected SplinkDataFrame at index {i}, "
                f"got {type(table).__name__}. "
                f"You must register your data using db_api.register() before passing "
                f"it to this function."
            )

    # Extract and validate db_api
    db_api = tables[0].db_api

    for i, table in enumerate(tables[1:], start=1):
        if table.db_api is not db_api:
            raise ValueError(
                f"All tables must be registered with the same db_api. "
                f"Table at index {i} has a different db_api than table at index 0."
            )

    return db_api


def splink_dataframes_to_dict(
    table_or_tables: Union["SplinkDataFrame", Sequence["SplinkDataFrame"]],
) -> Dict[str, "SplinkDataFrame"]:
    """Convert SplinkDataFrame input(s) to a dictionary keyed by templated_name.

    This is useful for functions that need the format returned by
    register_multiple_tables().

    Args:
        table_or_tables: A single SplinkDataFrame or a list of SplinkDataFrames

    Returns:
        Dictionary mapping templated_name to SplinkDataFrame
    """
    from splink.internals.splink_dataframe import SplinkDataFrame

    # Normalize to list
    if isinstance(table_or_tables, SplinkDataFrame):
        tables = [table_or_tables]
    else:
        tables = list(table_or_tables)

    return {sdf.templated_name: sdf for sdf in tables}
