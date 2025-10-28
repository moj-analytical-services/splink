import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError


@pytest.fixture(scope="session")
def _pg_credentials():
    # credentials for a role that will create + mangage the testing database
    # should have the CREATEDB privilege (or superuser)
    return {
        "user": os.environ.get("SPLINKTEST_PG_USER", "splinkognito"),
        "password": os.environ.get("SPLINKTEST_PG_PASSWORD", "splink123!"),
        "host": os.environ.get("SPLINKTEST_PG_HOST", "localhost"),
        "port": os.environ.get("SPLINKTEST_PG_PORT", "5432"),
        "db": os.environ.get("SPLINKTEST_PG_DB", "splink_db"),
    }


@pytest.fixture(scope="session")
def _engine_factory(_pg_credentials):
    def get_engine(
        db=_pg_credentials["db"],
        user=_pg_credentials["user"],
        pw=_pg_credentials["password"],
    ):
        try:
            return create_engine(
                f"postgresql+psycopg2://{user}:{pw}"
                f"@{_pg_credentials['host']}:{_pg_credentials['port']}/{db}"
            )
        except ModuleNotFoundError:
            return None

    return get_engine


def _setup_test_env(_engine_factory):
    # catch case where no postgres available - if testing other backends
    try:
        engine = _engine_factory()
        if engine is None:
            return None
        conn = engine.connect()
    except OperationalError as e:
        print(repr(e))  # noqa: T201
        return None
    uuid = str(uuid4()).replace("-", "_")
    db_name = f"__splink__testing_database_{uuid}"
    user = f"pytest_{uuid}"
    password = "testpw"
    create_db_sql = f"CREATE DATABASE {db_name}"
    # force drop as connections are persisting
    # would be good to relax by fixing connection issue, but doesn't matter in this env
    drop_db_sql = f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"
    drop_user_sql = f"DROP USER IF EXISTS {user}"

    conn.execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(text(drop_db_sql))
    conn.execute(text(create_db_sql))

    conn.execute(
        text(
            f"""
        CREATE USER {user} WITH
        PASSWORD '{password}'
        """
        )
    )
    # required permission - for extensions + making schema
    conn.execute(
        text(
            f"""
        GRANT CREATE ON DATABASE {db_name} TO {user};
        """
        )
    )

    new_conn = _engine_factory(db_name).connect()
    new_conn.execution_options(isolation_level="AUTOCOMMIT")
    # only need this for creating tables before being fed to Splink
    # don't need for actual Splink operation
    new_conn.execute(
        text(
            f"""
        GRANT CREATE ON SCHEMA public TO {user};
        """
        )
    )
    # for UDFs need these two:
    new_conn.execute(text(f"GRANT USAGE ON LANGUAGE SQL TO {user};"))
    new_conn.execute(text(f"GRANT USAGE ON TYPE float8 TO {user};"))
    new_conn.close()

    def teardown():
        conn.execute(text(drop_db_sql))
        conn.execute(text(drop_user_sql))
        conn.close()

    return {
        "teardown_func": teardown,
        "details": {"db": db_name, "user": user, "password": password},
    }


@pytest.fixture(scope="session")
def _postgres(_engine_factory):
    # this sets up/tears down the test database + our splink user within it
    conn_info = _setup_test_env(_engine_factory)
    if conn_info is None:
        yield None
    else:
        yield conn_info["details"]
        conn_info["teardown_func"]()


@pytest.fixture(scope="function")
def pg_engine(_engine_factory, _postgres):
    # catch case where no postgres available - if testing other backends
    if _postgres is None:
        yield None
    else:
        # user engine, for registering tables outside of Splink
        engine = _engine_factory(
            _postgres["db"], _postgres["user"], _postgres["password"]
        )
        yield engine

        engine.dispose()
