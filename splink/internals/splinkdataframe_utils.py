from __future__ import annotations

from collections.abc import Sequence as AbcSequence
from typing import TYPE_CHECKING, Any, List, Sequence

from splink.internals.exceptions import SplinkException
from splink.internals.splink_dataframe import SplinkDataFrame

if TYPE_CHECKING:
    from splink.internals.database_api import DatabaseAPI
    from splink.internals.splink_dataframe import SplinkDataFrame


def _fully_qualified_type_name(obj: Any) -> str:
    """Returns e.g. pandas.core.frame.DataFrame rather than just DataFrame"""
    cls = type(obj)
    qualname = getattr(cls, "__qualname__", getattr(cls, "__name__", repr(cls)))
    module = getattr(cls, "__module__", None)

    if not isinstance(module, str) or module in {"builtins", ""}:
        return qualname

    return f"{module}.{qualname}"


def _raise_not_a_splink_dataframe(obj: Any) -> None:
    """Raise a clear, actionable error when a non-SplinkDataFrame is passed where a
    SplinkDataFrame is required."""
    got = _fully_qualified_type_name(obj)
    raise TypeError(
        f"Expected a SplinkDataFrame but received a {got}.\n"
        "From Splink 5 onwards, input data must be registered with the database API "
        "before it is passed  to Splink. Registering a table returns a "
        "SplinkDataFrame:\n\n"
        '    df = db_api.register(my_data, dataset_display_name="my_dataset")\n'
        "    linker = Linker(df, settings)\n\n"
        "db_api.register() accepts a pandas DataFrame, a pyarrow Table, a "
        "dict[str, list], a list[dict], a backend-native object (such as a duckdb "
        "relation or Spark DataFrame), or a string naming a table that already "
        "exists in the backend."
    )


def _ensure_splink_dataframes(
    table_or_tables: SplinkDataFrame | Sequence[SplinkDataFrame],
) -> List[SplinkDataFrame]:
    """Normalise the input into a list of SplinkDataFrames, raising a clear error if
    any input is not a SplinkDataFrame."""
    if isinstance(table_or_tables, SplinkDataFrame):
        tables = [table_or_tables]
    elif isinstance(table_or_tables, AbcSequence) and not isinstance(
        table_or_tables, (str, bytes)
    ):
        tables = list(table_or_tables)
    else:
        # Any other type (e.g. a pandas DataFrame, pyarrow Table, dict) is a single
        # invalid input. Report it directly rather than trying to iterate it.
        _raise_not_a_splink_dataframe(table_or_tables)

    for table in tables:
        if not isinstance(table, SplinkDataFrame):
            _raise_not_a_splink_dataframe(table)

    return tables


def get_db_api_from_inputs(
    table_or_tables: SplinkDataFrame | Sequence[SplinkDataFrame],
) -> DatabaseAPI[Any]:
    tables = _ensure_splink_dataframes(table_or_tables)

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
    tables = _ensure_splink_dataframes(table_or_tables)

    return {sdf.templated_name: sdf for sdf in tables}
