import os
import tempfile
import uuid

import duckdb


def validate_duckdb_connection(connection, logger):
    """Check if the duckdb connection requested by the user is valid.

    Raises:
        Exception: If the connection is invalid or a warning if
        the naming convention is ambiguous (not adhering to the
        duckdb convention).
    """

    if isinstance(connection, duckdb.DuckDBPyConnection):
        return

    if not isinstance(connection, str):
        raise Exception(
            "Connection must be a string in the form: :memory:, :temporary: "
            "or the name of a new or existing duckdb database."
        )

    connection = connection.lower()

    if connection in [":memory:", ":temporary:", ":default:"]:
        return

    suffixes = (".duckdb", ".db")
    if connection.endswith(suffixes):
        return

    logger.info(
        f"The registered connection -- {connection} -- has an uncommon file type. "
        "We recommend that you add a clear suffix of '.db' or '.duckdb' "
        "to the connection string, when generating an on-disk database."
    )


def create_temporary_duckdb_connection(self):
    """
    Create a temporary duckdb connection.
    """
    self._temp_dir = tempfile.TemporaryDirectory(dir="")
    fname = uuid.uuid4().hex[:7]
    path = os.path.join(self._temp_dir.name, f"{fname}.duckdb")
    con = duckdb.connect(database=path, read_only=False)
    return con
