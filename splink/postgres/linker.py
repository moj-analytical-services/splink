from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..input_column import InputColumn
from ..linker import Linker
from ..misc import ensure_is_list
from ..splink_dataframe import SplinkDataFrame
from ..unique_id_concat import _composite_unique_id_from_nodes_sql

logger = logging.getLogger(__name__)


class PostgresDataFrame(SplinkDataFrame):
    linker: PostgresLinker

    def __init__(self, df_name, physical_name, linker):
        super().__init__(df_name, physical_name, linker)
        self._db_schema = linker._db_schema
        self.physical_name = f"{self.physical_name}"

    @property
    def columns(self) -> list[InputColumn]:
        sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{self.physical_name}';
        """
        res = self.linker._run_sql_execution(sql).fetchall()
        cols = [r["column_name"] for r in res]

        return [InputColumn(c, sql_dialect="postgres") for c in cols]

    def validate(self):
        if type(self.physical_name) is not str:
            raise ValueError(
                f"{self.df_name} is not a string dataframe.\n"
                "Postgres Linker requires input data"
                " to be a string containing the name of the"
                " postgres table."
            )

        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = '{self.physical_name}';
        """

        res = self.linker._run_sql_execution(sql).fetchall()
        if len(res) == 0:
            raise ValueError(
                f"{self.physical_name} does not exist in the postgres db provided.\n"
                "Postgres Linker requires input data"
                " to be a string containing the name of a"
                " postgres table that exists in the provided db."
            )

    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)
        self.linker._delete_table_from_database(self.physical_name)

    def as_record_dict(self, limit=None):
        sql = f"""
        SELECT *
        FROM {self.physical_name}
        """
        if limit:
            sql += f" LIMIT {limit}"
        sql += ";"
        res = self.linker._run_sql_execution(sql).mappings().all()
        return [dict(r) for r in res]


class PostgresLinker(Linker):
    def __init__(
        self,
        input_table_or_tables,
        settings_dict=None,
        engine: Engine = None,
        set_up_basic_logging=True,
        input_table_aliases: str | list = None,
        validate_settings: bool = True,
        schema="splink",
        other_schemas_to_search: str | list = [],
    ):
        self._sql_dialect_ = "postgres"
        if not isinstance(engine, Engine):
            raise ValueError(
                "You must supply a sqlalchemy engine " "to create a PostgresLinker."
            )

        self._engine = engine

        input_tables = ensure_is_list(input_table_or_tables)
        input_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )
        accepted_df_dtypes = pd.DataFrame
        self._db_schema = schema
        # Create splink schema
        self._create_splink_schema(other_schemas_to_search)

        # Create custom SQL functions in database
        self._register_custom_functions()
        self._register_extensions()

        super().__init__(
            input_tables,
            settings_dict,
            accepted_df_dtypes,
            set_up_basic_logging,
            input_table_aliases=input_aliases,
            validate_settings=validate_settings,
        )

    def _table_to_splink_dataframe(self, templated_name, physical_name):
        return PostgresDataFrame(templated_name, physical_name, self)

    def _execute_sql_against_backend(self, sql, templated_name, physical_name):
        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self._delete_table_from_database(physical_name)

        sql = f"CREATE TABLE {physical_name} AS {sql}"
        self._log_and_run_sql_execution(sql, templated_name, physical_name)

        return self._table_to_splink_dataframe(templated_name, physical_name)

    def _run_sql_execution(
        self, final_sql: str, templated_name: str = None, physical_name: str = None
    ):
        with self._engine.connect() as con:
            res = con.execute(text(final_sql))
        return res

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Will error if an invalid data type is passed
        input.to_sql(
            table_name,
            con=self._engine,
            index=False,
            if_exists="replace",
            schema=self._db_schema,
        )

    def register_table(self, input, table_name, overwrite=False):
        # If the user has provided a table name, return it as a SplinkDataframe
        if isinstance(input, str):
            return self._table_to_splink_dataframe(table_name, input)

        # Check if table name is already in use
        exists = self._table_exists_in_database(table_name)
        if exists:
            if not overwrite:
                raise ValueError(
                    f"Table '{table_name}' already exists in database. "
                    "Please use the 'overwrite' argument if you wish to overwrite"
                )
            else:
                self._delete_table_from_database(table_name)

        self._table_registration(input, table_name)
        return self._table_to_splink_dataframe(table_name, table_name)

    def _random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        if seed:
            # TODO: we could maybe do seeds by handling it in calling function
            # need to execute setseed() in surrounding session
            raise NotImplementedError(
                "Postgres does not support seeds in random "
                "samples. Please remove the `seed` parameter."
            )

        sample_size = int(sample_size)

        if unique_id is None:
            # unique_id col, with source_dataset column if needed to disambiguate
            unique_id_cols = self._settings_obj._unique_id_input_columns
            unique_id = _composite_unique_id_from_nodes_sql(unique_id_cols)
        if table is None:
            table = "__splink__df_concat_with_tf"
        return (
            f"WHERE {unique_id} IN ("
            f"    SELECT {unique_id} FROM {table}"
            f"    ORDER BY RANDOM() LIMIT {sample_size}"
            f")"
        )

    @property
    def _infinity_expression(self):
        return "'infinity'"

    def _table_exists_in_database(self, table_name):
        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = '{table_name}';
        """

        rec = self._run_sql_execution(sql).fetchall()
        return len(rec) > 0

    def _delete_table_from_database(self, name):
        drop_sql = f"DROP TABLE IF EXISTS {name};"
        self._run_sql_execution(drop_sql)

    def _create_log2_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION log2(n float8)
        RETURNS float8 AS $$
        SELECT log(2.0, n::numeric)::float8;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

    def _extend_round_function(self):
        # extension of round to double
        sql = """
        CREATE OR REPLACE FUNCTION round(n float8, dp integer)
        RETURNS numeric AS $$
        SELECT round(n::numeric, dp);
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

    def _create_datediff_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION datediff(x date, y date)
        RETURNS integer AS $$
        SELECT x - y;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

        sql_cast = """
        CREATE OR REPLACE FUNCTION datediff(x {dateish_type}, y {dateish_type})
        RETURNS integer AS $$
        SELECT datediff(DATE(x), DATE(y));
        $$ LANGUAGE SQL IMMUTABLE;
        """
        for dateish_type in ("timestamp", "timestamp with time zone"):
            self._run_sql_execution(sql_cast.format(dateish_type=dateish_type))

    def _create_months_between_function(self):
        # number of average-length (per year) months between two dates
        # logic could be improved/made consistent with other backends
        # but this is reasonable for now
        # 30.4375 days
        ave_length_month = 365.25 / 12
        sql = f"""
        CREATE OR REPLACE FUNCTION ave_months_between(x date, y date)
        RETURNS float8 AS $$
        SELECT (datediff(x, y)/{ave_length_month})::float8;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

        sql_cast = """
        CREATE OR REPLACE FUNCTION ave_months_between(
            x {dateish_type}, y {dateish_type}
        )
        RETURNS integer AS $$
        SELECT (ave_months_between(DATE(x), DATE(y)))::int;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        for dateish_type in ("timestamp", "timestamp with time zone"):
            self._run_sql_execution(sql_cast.format(dateish_type=dateish_type))

    def _create_array_intersect_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION array_intersect(x anyarray, y anyarray)
        RETURNS anyarray AS $$
        SELECT ARRAY( SELECT DISTINCT * FROM UNNEST(x) WHERE UNNEST = ANY(y) )
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._run_sql_execution(sql)

    def _register_custom_functions(self):
        # if people have issues with permissions we can allow these to be optional
        # need for predict_from_comparison_vectors_sql (could adjust)
        self._create_log2_function()
        # need for datediff levels
        self._create_datediff_function()
        self._create_months_between_function()
        # need for array_intersect levels
        self._create_array_intersect_function()
        # extension of round to handle doubles - used in unlinkables
        self._extend_round_function()

    def _register_extensions(self):
        sql = """
        CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
        """
        self._run_sql_execution(sql)

    def _create_splink_schema(self, other_schemas_to_search):
        other_schemas_to_search = ensure_is_list(other_schemas_to_search)
        # always search _db_schema first, and public last
        schemas_to_search = [self._db_schema] + other_schemas_to_search + ["public"]
        search_path = ",".join(schemas_to_search)
        sql = f"""
        CREATE SCHEMA IF NOT EXISTS {self._db_schema};
        SET search_path TO {search_path};
        """
        self._run_sql_execution(sql)
