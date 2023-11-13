from __future__ import annotations

import warnings

from .comparison_level import ComparisonLevel
from .comparison_level_sql import great_circle_distance_km_sql
from .input_column import InputColumn


class NullLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name,
        valid_string_pattern: str = None,
        invalid_dates_as_null: bool = False,
        valid_string_regex: str = None,
    ) -> ComparisonLevel:
        """Represents comparisons level where one or both sides of the comparison
        contains null values so the similarity cannot be evaluated.
        Assumed to have a partial match weight of zero (null effect
        on overall match weight)
        Args:
            col_name (str): Input column name
            valid_string_pattern (str): pattern (regex or otherwise) that if not
                matched will result in column being treated as a null.
            invalid_dates_as_null (bool): If True, set all invalid dates to null.
                The "correct" format of a date is set by valid_string_pattern.
                Defaults to false.

        Examples:
            === ":simple-duckdb: DuckDB"
                Simple null comparison level
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.null_level("name")
                ```
                Null comparison level including strings that do not match
                a given regex pattern
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.null_level("name", valid_string_pattern="^[A-Z]{1,7}$")
                ```
            === ":simple-apachespark: Spark"
                Simple null level
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.null_level("name")
                ```
                Null comparison level including strings that do not match
                a given regex pattern
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.null_level("name", valid_string_pattern="^[A-Z]{1,7}$")
                ```
            === ":simple-amazonaws: Athena"
                Simple null level
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.null_level("name")
                ```
                Null comparison level including strings that do not match
                a given regex pattern
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.null_level("name", valid_string_pattern="^[A-Z]{1,7}$")
                ```
            === ":simple-sqlite: SQLite"
                Simple null level
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.null_level("name")
                ```
            === ":simple-postgresql: PostgreSql"
                Simple null level
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.null_level("name")
                ```
        Returns:
            ComparisonLevel: Comparison level for null entries
        """

        # TODO: Remove this compatibility code in a future release once we drop
        # support for "valid_string_regex". Deprecation warning added in 3.9.6
        if valid_string_pattern is not None and valid_string_regex is not None:
            # user supplied both
            raise TypeError("Just use valid_string_pattern")
        elif valid_string_pattern is not None:
            # user is doing it correctly
            pass
        elif valid_string_regex is not None:
            # user is using deprecated argument
            warnings.warn(
                "valid_string_regex is deprecated; use valid_string_pattern",
                DeprecationWarning,
                stacklevel=2,
            )
            valid_string_pattern = valid_string_regex

        col = InputColumn(col_name, sql_dialect=self._sql_dialect)
        col_name_l, col_name_r = col.name_l, col.name_r

        if invalid_dates_as_null:
            col_name_l = self._valid_date_function(col_name_l, valid_string_pattern)
            col_name_r = self._valid_date_function(col_name_r, valid_string_pattern)
            sql = f"""{col_name_l} IS NULL OR {col_name_r} IS NULL OR
                      {col_name_l}=='' OR {col_name_r} ==''"""
        elif valid_string_pattern:
            col_name_l = self._regex_extract_function(col_name_l, valid_string_pattern)
            col_name_r = self._regex_extract_function(col_name_r, valid_string_pattern)
            sql = f"""{col_name_l} IS NULL OR {col_name_r} IS NULL OR
                      {col_name_l}=='' OR {col_name_r} ==''"""
        else:
            sql = f"{col_name_l} IS NULL OR {col_name_r} IS NULL"

        level_dict = {
            "sql_condition": sql,
            "label_for_charts": "Null",
            "is_null_level": True,
        }
        super().__init__(level_dict, sql_dialect=self._sql_dialect)


class ExactMatchLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name,
        regex_extract: str = None,
        set_to_lowercase: bool = False,
        m_probability=None,
        term_frequency_adjustments=False,
        include_colname_in_charts_label=False,
        manual_col_name_for_charts_label=None,
    ) -> ComparisonLevel:
        """Represents a comparison level where there is an exact match,

        Args:
            col_name (str): Input column name
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, sets all entries to lowercase.
            m_probability (float, optional): Starting value for m probability
                Defaults to None.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            include_colname_in_charts_label (bool, optional): If True, include col_name
                in chart labels (e.g. linker.match_weights_chart())
            manual_col_name_for_charts_label (str, optional): string to include as
                 column name in chart label. Acts as a manual overwrite of the
                 colname when include_colname_in_charts_label is True.
                include_colname_in_charts_label=True
        Examples:
            === ":simple-duckdb: DuckDB"
                Simple Exact match level
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.exact_match_level("name")
                ```
                Exact match level with term-frequency adjustments
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.exact_match_level("name", term_frequency_adjustments=True)
                ```
                Exact match level on a substring of col_name as
                 determined by a regular expression
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.exact_match_level("name", regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-apachespark: Spark"
                Simple Exact match level
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.exact_match_level("name")
                ```
                Exact match level with term-frequency adjustments
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.exact_match_level("name", term_frequency_adjustments=True)
                ```
                Exact match level on a substring of col_name as
                 determined by a regular expression
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.exact_match_level("name", regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-amazonaws: Athena"
                Simple Exact match level
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.exact_match_level("name")
                ```
                Exact match level with term-frequency adjustments
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.exact_match_level("name", term_frequency_adjustments=True)
                ```
                Exact match level on a substring of col_name as
                 determined by a regular expression
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.exact_match_level("name", regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-sqlite: SQLite"
                Simple Exact match level
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.exact_match_level("name")
                ```
                Exact match level with term-frequency adjustments
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.exact_match_level("name", term_frequency_adjustments=True)
            === ":simple-postgresql: PostgreSql"
                Simple Exact match level
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.exact_match_level("name")
                ```
                Exact match level with term-frequency adjustments
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.exact_match_level("name", term_frequency_adjustments=True)
                ```
        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)

        if include_colname_in_charts_label:
            label_suffix = f" {col_name}"
        elif manual_col_name_for_charts_label:
            label_suffix = f" {manual_col_name_for_charts_label}"
        else:
            label_suffix = ""

        col_name_l, col_name_r = col.name_l, col.name_r

        if set_to_lowercase:
            col_name_l = f"lower({col_name_l})"
            col_name_r = f"lower({col_name_r})"

        if regex_extract:
            col_name_l = self._regex_extract_function(col_name_l, regex_extract)
            col_name_r = self._regex_extract_function(col_name_r, regex_extract)

        sql_cond = f"{col_name_l} = {col_name_r}"
        level_dict = {
            "sql_condition": sql_cond,
            "label_for_charts": f"Exact match{label_suffix}",
        }
        if m_probability:
            level_dict["m_probability"] = m_probability
        if term_frequency_adjustments:
            level_dict["tf_adjustment_column"] = col_name

        super().__init__(level_dict, sql_dialect=self._sql_dialect)


class ElseLevelBase(ComparisonLevel):
    def __init__(
        self,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level for all cases which have not been
        considered by preceding comparison levels,

        Examples:
            === ":simple-duckdb: DuckDB"
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.else_level()
                ```
            === ":simple-apachespark: Spark"
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.else_level()
                ```
            === ":simple-amazonaws: Athena"
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.else_level()
                ```
            === ":simple-sqlite: SQLite"
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.else_level()
            === ":simple-postgresql: PostgreSql"
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.else_level()
                ```
        """
        if isinstance(m_probability, str):
            raise ValueError(
                "You provided a string for the value of m probability when it should "
                "be numeric.  Perhaps you passed a column name.  Note that you do "
                "not need to pass a column name into the else level."
            )
        level_dict = {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
        }
        if m_probability:
            level_dict["m_probability"] = m_probability
        super().__init__(level_dict)


class DistanceFunctionLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name: str,
        distance_function_name: str,
        distance_threshold: int | float,
        regex_extract: str = None,
        set_to_lowercase=False,
        higher_is_more_similar: bool = True,
        include_colname_in_charts_label=False,
        manual_col_name_for_charts_label=None,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using a user-provided distance function,
        where the similarity

        Args:
            col_name (str): Input column name
            distance_function_name (str): The name of the distance function
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, sets all entries to lowercase.
            higher_is_more_similar (bool): If True, a higher value of the
                distance function indicates a higher similarity (e.g. jaro_winkler).
                If false, a higher value indicates a lower similarity
                (e.g. levenshtein).
            include_colname_in_charts_label (bool, optional): If True, includes
                col_name in charts label.
            manual_col_name_for_charts_label (str, optional): string to include as
                 column name in chart label. Acts as a manual overwrite of the
                 colname when include_colname_in_charts_label is True.
            m_probability (float, optional): Starting value for m probability
                Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Apply the `levenshtein` function to a comparison level
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.distance_function_level("name",
                                            "levenshtein",
                                            2,
                                            False)
                ```
            === ":simple-apachespark: Spark"
                Apply the `levenshtein` function to a comparison level
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.distance_function_level("name",
                                            "levenshtein",
                                            2,
                                            False)
                ```
            === ":simple-amazonaws: Athena"
                Apply the `levenshtein_distance` function to a comparison level
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.distance_function_level("name",
                                            "levenshtein_distance",
                                            2,
                                            False)
                ```
            === ":simple-sqlite: SQLite"
                Apply the `levenshtein` function to a comparison level
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.distance_function_level("name",
                                            "levenshtein",
                                            2,
                                            False)
                ```
            === ":simple-postgresql: PostgreSql"
                Apply the `levenshtein` function to a comparison level
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.distance_function_level("name",
                                            "levenshtein",
                                            2,
                                            False)
                ```

        Returns:
            ComparisonLevel: A comparison level for a given distance function
        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)

        if higher_is_more_similar:
            operator = ">="
        else:
            operator = "<="

        col_name_l, col_name_r = col.name_l, col.name_r

        if set_to_lowercase:
            col_name_l = f"lower({col_name_l})"
            col_name_r = f"lower({col_name_r})"

        if regex_extract:
            col_name_l = self._regex_extract_function(col_name_l, regex_extract)
            col_name_r = self._regex_extract_function(col_name_r, regex_extract)

        sql_cond = (
            f"{distance_function_name}({col_name_l}, {col_name_r}) "
            f"{operator} {distance_threshold}"
        )

        if include_colname_in_charts_label:
            if manual_col_name_for_charts_label:
                col_name = manual_col_name_for_charts_label

            label_suffix = f" {col_name}"
        else:
            label_suffix = ""

        chart_label = (
            f"{distance_function_name.capitalize()}{label_suffix} {operator} "
            f"{distance_threshold}"
        )

        level_dict = {
            "sql_condition": sql_cond,
            "label_for_charts": chart_label,
        }
        if m_probability:
            level_dict["m_probability"] = m_probability

        super().__init__(level_dict, sql_dialect=self._sql_dialect)

    @property
    def _distance_level(self):
        raise NotImplementedError("Distance function not supported in this dialect")


class LevenshteinLevelBase(DistanceFunctionLevelBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold: int,
        regex_extract: str = None,
        set_to_lowercase=False,
        include_colname_in_charts_label=False,
        manual_col_name_for_charts_label=None,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using a levenshtein distance function,

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, sets all entries to lowercase.
            include_colname_in_charts_label (bool, optional): If True, includes
                col_name in charts label
            manual_col_name_for_charts_label (str, optional): string to include as
                 column name in chart label. Acts as a manual overwrite of the
                 colname when include_colname_in_charts_label is True.
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Comparison level with levenshtein distance score less than (or equal
                 to) 1
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.levenshtein_level("name", 1)
                ```

                Comparison level with levenshtein distance score less than (or equal
                 to) 1 on a subtring of name column as determined by a regular
                expression.
                ```python
                import splink.duckdb.comparison_level_library as cll
                cll.levenshtein_level("name", 1, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-apachespark: Spark"
                Comparison level with levenshtein distance score less than (or equal
                 to) 1
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.levenshtein_level("name", 1)
                ```

                Comparison level with levenshtein distance score less than (or equal
                 to) 1 on a subtring of name column as determined by a regular
                expression.
                ```python
                import splink.spark.comparison_level_library as cll
                cll.levenshtein_level("name", 1, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-amazonaws: Athena"
                Comparison level with levenshtein distance score less than (or equal
                 to) 1
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.levenshtein_level("name", 1)
                ```

                Comparison level with levenshtein distance score less than (or equal
                 to) 1 on a subtring of name column as determined by a regular
                expression.
                ```python
                import splink.athena.comparison_level_library as cll
                cll.levenshtein_level("name", 1, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-sqlite: SQLite"
                Comparison level with levenshtein distance score less than (or equal
                 to) 1
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.levenshtein_level("name", 1)
                ```
            === ":simple-postgresql: PostgreSql"
                Comparison level with levenshtein distance score less than (or equal
                 to) 1
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.levenshtein_level("name", 1)
                ```

        Returns:
            ComparisonLevel: A comparison level that evaluates the
                levenshtein similarity
        """
        super().__init__(
            col_name,
            distance_function_name=self._levenshtein_name,
            distance_threshold=distance_threshold,
            regex_extract=regex_extract,
            set_to_lowercase=set_to_lowercase,
            higher_is_more_similar=False,
            include_colname_in_charts_label=include_colname_in_charts_label,
            m_probability=m_probability,
        )


class DamerauLevenshteinLevelBase(DistanceFunctionLevelBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold: int,
        regex_extract: str = None,
        set_to_lowercase=False,
        include_colname_in_charts_label=False,
        manual_col_name_for_charts_label=None,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using a damerau-levenshtein distance
        function,

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, sets all entries to lowercase.
            include_colname_in_charts_label (bool, optional): If True, includes
                col_name in charts label
            manual_col_name_for_charts_label (str, optional): string to include as
                 column name in chart label. Acts as a manual overwrite of the
                 colname when include_colname_in_charts_label is True.
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Comparison level with damerau-levenshtein distance score less than
                (or equal to) 1
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.damerau_levenshtein_level("name", 1)
                ```

                Comparison level with damerau-levenshtein distance score less than
                (or equal to) 1 on a subtring of name column as determined by a regular
                expression.
                ```python
                import splink.duckdb.comparison_level_library as cll
                cll.damerau_levenshtein_level("name", 1, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-apachespark: Spark"
                Comparison level with damerau-levenshtein distance score less than
                (or equal to) 1
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.damerau_levenshtein_level("name", 1)
                ```

                Comparison level with damerau-levenshtein distance score less than
                (or equal to) 1 on a subtring of name column as determined by a regular
                expression.
                ```python
                import splink.spark.comparison_level_library as cll
                cll.damerau_levenshtein_level("name", 1, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-sqlite: SQLite"
                Comparison level with damerau-levenshtein distance score less than
                (or equal to) 1
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.damerau_levenshtein_level("name", 1)
                ```

        Returns:
            ComparisonLevel: A comparison level that evaluates the
                Damerau-Levenshtein similarity
        """
        super().__init__(
            col_name,
            distance_function_name=self._damerau_levenshtein_name,
            distance_threshold=distance_threshold,
            regex_extract=regex_extract,
            set_to_lowercase=set_to_lowercase,
            higher_is_more_similar=False,
            include_colname_in_charts_label=include_colname_in_charts_label,
            m_probability=m_probability,
        )


class JaroLevelBase(DistanceFunctionLevelBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold: float,
        regex_extract: str = None,
        set_to_lowercase=False,
        include_colname_in_charts_label=False,
        manual_col_name_for_charts_label=None,
        m_probability=None,
    ):
        """Represents a comparison using the jaro distance function

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, sets all entries to lowercase.
            include_colname_in_charts_label (bool, optional): If True, includes
                col_name in charts label
            manual_col_name_for_charts_label (str, optional): string to include as
                 column name in chart label. Acts as a manual overwrite of the
                 colname when include_colname_in_charts_label is True.
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Comparison level with jaro score greater than 0.9
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.jaro_level("name", 0.9)
                ```
                Comparison level with a jaro score greater than 0.9 on a substring
                of name column as determined by a regular expression.

                ```python
                import splink.duckdb.comparison_level_library as cll
                cll.jaro_level("name", 0.9, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-apachespark: Spark"
                Comparison level with jaro score greater than 0.9
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.jaro_level("name", 0.9)
                ```
                Comparison level with a jaro score greater than 0.9 on a substring
                of name column as determined by a regular expression.

                ```python
                import splink.spark.comparison_level_library as cll
                cll.jaro_level("name", 0.9, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-sqlite: SQLite"
                Comparison level with jaro score greater than 0.9
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.jaro_level("name", 0.9)
                ```

        Returns:
            ComparisonLevel: A comparison level that evaluates the
                jaro similarity
        """

        super().__init__(
            col_name,
            self._jaro_name,
            distance_threshold=distance_threshold,
            regex_extract=regex_extract,
            set_to_lowercase=set_to_lowercase,
            higher_is_more_similar=True,
            include_colname_in_charts_label=include_colname_in_charts_label,
            m_probability=m_probability,
        )


class JaroWinklerLevelBase(DistanceFunctionLevelBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold: float,
        regex_extract: str = None,
        set_to_lowercase=False,
        include_colname_in_charts_label=False,
        manual_col_name_for_charts_label=None,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using the jaro winkler distance function

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, sets all entries to lowercase.
            include_colname_in_charts_label (bool, optional): If True, includes
                col_name in charts label
            manual_col_name_for_charts_label (str, optional): string to include as
                 column name in chart label. Acts as a manual overwrite of the
                 colname when include_colname_in_charts_label is True.
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Comparison level with jaro-winkler score greater than 0.9
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.jaro_winkler_level("name", 0.9)
                ```
                Comparison level with jaro-winkler score greater than 0.9 on a
                substring of name column as determined by a regular expression.
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.jaro_winkler_level("name", 0.9, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-apachespark: Spark"
                Comparison level with jaro-winkler score greater than 0.9
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.jaro_winkler_level("name", 0.9)
                ```
                Comparison level with jaro-winkler score greater than 0.9 on a
                substring of name column as determined by a regular expression.
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.jaro_winkler_level("name", 0.9, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-sqlite: SQLite"
                Comparison level with jaro-winkler score greater than 0.9
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.jaro_winkler_level("name", 0.9)
                ```

        Returns:
            ComparisonLevel: A comparison level that evaluates the
                jaro winkler similarity
        """

        super().__init__(
            col_name,
            self._jaro_winkler_name,
            distance_threshold=distance_threshold,
            regex_extract=regex_extract,
            set_to_lowercase=set_to_lowercase,
            higher_is_more_similar=True,
            include_colname_in_charts_label=include_colname_in_charts_label,
            m_probability=m_probability,
        )

    @property
    def _jaro_winkler_name(self):
        raise NotImplementedError(
            "Jaro-winkler function name not defined on base class"
        )


class JaccardLevelBase(DistanceFunctionLevelBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold: int | float,
        regex_extract: str = None,
        set_to_lowercase=False,
        include_colname_in_charts_label=False,
        manual_col_name_for_charts_label=None,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using a jaccard distance function

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, sets all entries to lowercase.
            include_colname_in_charts_label (bool, optional): If True, includes
                col_name in charts label
            manual_col_name_for_charts_label (str, optional): string to include as
                 column name in chart label. Acts as a manual overwrite of the
                 colname when include_colname_in_charts_label is True.
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.
        Examples:
            === ":simple-duckdb: DuckDB"
                Comparison level with jaccard score greater than 0.9
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.jaccard_level("name", 0.9)
                ```
                Comparison level with jaccard score greater than 0.9 on a
                substring of name column as determined by a regular expression.
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.jaccard_level("name", 0.9, regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-apachespark: Spark"
                Comparison level with jaccard score greater than 0.9
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.jaccard_level("name", 0.9)
                ```
                Comparison level with jaccard score greater than 0.9 on a
                substring of name column as determined by a regular expression.
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.jaccard_level("name", 0.9, regex_extract="^[A-Z]{1,4}")
                ```

        Returns:
            ComparisonLevel: A comparison level that evaluates the jaccard similarity
        """
        super().__init__(
            col_name,
            self._jaccard_name,
            distance_threshold=distance_threshold,
            regex_extract=regex_extract,
            set_to_lowercase=set_to_lowercase,
            higher_is_more_similar=True,
            include_colname_in_charts_label=include_colname_in_charts_label,
            m_probability=m_probability,
        )


class ColumnsReversedLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name_1: str,
        col_name_2: str,
        regex_extract: str = None,
        set_to_lowercase=False,
        m_probability=None,
        tf_adjustment_column=None,
    ) -> ComparisonLevel:
        """Represents a comparison level where the columns are reversed.  For example,
        if surname is in the forename field and vice versa

        Args:
            col_name_1 (str): First column, e.g. forename
            col_name_2 (str): Second column, e.g. surname
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, sets all entries to lowercase.
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.
            tf_adjustment_column (str, optional): Column to use for term frequency
                adjustments if an exact match is observed. Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Comparison level on first_name and surname columns reversed
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.columns_reversed_level("first_name", "surname")
                ```
                Comparison level on first_name and surname column reversed
                on a substring of each column as determined by a regular expression.
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.columns_reversed_level("first_name",
                                           "surname",
                                           regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-apachespark: Spark"
                Comparison level on first_name and surname columns reversed
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.columns_reversed_level("first_name", "surname")
                ```
                Comparison level on first_name and surname column reversed
                on a substring of each column as determined by a regular expression.
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.columns_reversed_level("first_name",
                                           "surname",
                                           regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-amazonaws: Athena"
                Comparison level on first_name and surname columns reversed
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.columns_reversed_level("first_name", "surname")
                ```
                Comparison level on first_name and surname column reversed
                on a substring of each column as determined by a regular expression.
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.columns_reversed_level("first_name",
                                           "surname",
                                           regex_extract="^[A-Z]{1,4}")
                ```
            === ":simple-sqlite: SQLite"
                Comparison level on first_name and surname columns reversed
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.columns_reversed_level("first_name", "surname")
                ```
            === ":simple-postgresql: PostgreSql"
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.columns_reversed_level("first_name", "surname")
                ```


        Returns:
            ComparisonLevel: A comparison level that evaluates the exact match of two
                columns.
        """

        col_1 = InputColumn(col_name_1, sql_dialect=self._sql_dialect)
        col_2 = InputColumn(col_name_2, sql_dialect=self._sql_dialect)

        col_1_l, col_1_r = col_1.name_l, col_1.name_r
        col_2_l, col_2_r = col_2.name_l, col_2.name_r

        if set_to_lowercase:
            col_1_l = f"lower({col_1_l})"
            col_1_r = f"lower({col_1_r})"
            col_2_l = f"lower({col_2_l})"
            col_2_r = f"lower({col_2_r})"

        if regex_extract:
            col_1_l = self._regex_extract_function(col_1_l, regex_extract)
            col_1_r = self._regex_extract_function(col_1_r, regex_extract)
            col_2_l = self._regex_extract_function(col_2_l, regex_extract)
            col_2_r = self._regex_extract_function(col_2_r, regex_extract)

        s = f"{col_1_l} = {col_2_r} and " f"{col_1_r} = {col_2_l}"
        level_dict = {
            "sql_condition": s,
            "label_for_charts": "Exact match on reversed cols",
        }
        if m_probability:
            level_dict["m_probability"] = m_probability

        if tf_adjustment_column:
            level_dict["tf_adjustment_column"] = tf_adjustment_column

        super().__init__(level_dict, sql_dialect=self._sql_dialect)


class DistanceInKMLevelBase(ComparisonLevel):
    def __init__(
        self,
        lat_col: str,
        long_col: str,
        km_threshold: int | float,
        not_null: bool = False,
        m_probability=None,
    ) -> ComparisonLevel:
        """Use the haversine formula to transform comparisons of lat,lngs
        into distances measured in kilometers

        Arguments:
            lat_col (str): The name of a latitude column or the respective array
                or struct column column containing the information
                For example: long_lat['lat'] or long_lat[0]
            long_col (str): The name of a longitudinal column or the respective array
                or struct column column containing the information, plus an index.
                For example: long_lat['long'] or long_lat[1]
            km_threshold (int): The total distance in kilometers to evaluate your
                comparisons against
            not_null (bool): If true, remove any . This is only necessary if you are not
                capturing nulls elsewhere in your comparison level.
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.distance_in_km_level("lat_col",
                                        "long_col",
                                        km_threshold=5)
                ```
            === ":simple-apachespark: Spark"
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.distance_in_km_level("lat_col",
                                        "long_col",
                                        km_threshold=5)
                ```
            === ":simple-amazonaws: Athena"
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.distance_in_km_level("lat_col",
                                        "long_col",
                                        km_threshold=5)
                ```
            === ":simple-postgresql: PostgreSql"
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.distance_in_km_level("lat_col",
                                        "long_col",
                                        km_threshold=5)
                ```

        Returns:
            ComparisonLevel: A comparison level that evaluates the distance between
                two coordinates
        """

        lat = InputColumn(lat_col, sql_dialect=self._sql_dialect)
        long = InputColumn(long_col, sql_dialect=self._sql_dialect)
        lat_l, lat_r = lat.names_l_r
        long_l, long_r = long.names_l_r

        distance_km_sql = f"""
        {great_circle_distance_km_sql(lat_l, lat_r, long_l, long_r)} <= {km_threshold}
        """

        if not_null:
            null_sql = " AND ".join(
                [f"{c} is not null" for c in [lat_r, lat_l, long_l, long_r]]
            )
            distance_km_sql = f"({null_sql}) AND {distance_km_sql}"

        level_dict = {
            "sql_condition": distance_km_sql,
            "label_for_charts": f"Distance less than {km_threshold}km",
        }

        if m_probability:
            level_dict["m_probability"] = m_probability

        super().__init__(level_dict, sql_dialect=self._sql_dialect)


class PercentageDifferenceLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name: str,
        percentage_distance_threshold: float,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level based around the percentage difference between
        two numbers.

        Note: the percentage is calculated by dividing the absolute difference between
        the values by the largest value

        Args:
            col_name (str): Input column name
            percentage_distance_threshold (float): Percentage difference threshold for
                the comparison level
            m_probability (float, optional): Starting value for m probability. Defaults
                to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.percentage_difference_level("value", 0.5)
                ```
            === ":simple-apachespark: Spark"
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.percentage_difference_level("value", 0.5)
                ```
            === ":simple-amazonaws: Athena"
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.percentage_difference_level("value", 0.5)
                ```
            === ":simple-sqlite: SQLite"
                ``` python
                import splink.sqlite.comparison_level_library as cll
                cll.percentage_difference_level("value", 0.5)
                ```
            === ":simple-postgresql: PostgreSql"
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.percentage_difference_level("value", 0.5)
                ```

        Returns:
            ComparisonLevel: A comparison level that evaluates the percentage difference
                between two values

        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)

        s = f"""(abs({col.name_l} - {col.name_r})/
            (case
                when {col.name_r} > {col.name_l}
                then {col.name_r}
                else {col.name_l}
            end))
            < {percentage_distance_threshold}"""

        level_dict = {
            "sql_condition": s,
            "label_for_charts": f"< {percentage_distance_threshold:,.2%} diff",
        }
        if m_probability:
            level_dict["m_probability"] = m_probability

        super().__init__(level_dict, sql_dialect=self._sql_dialect)


class ArrayIntersectLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name,
        m_probability=None,
        term_frequency_adjustments=False,
        min_intersection=1,
        include_colname_in_charts_label=False,
    ) -> ComparisonLevel:
        """Represents a comparison level based around the size of an intersection of
        arrays

        Args:
            col_name (str): Input column name
            m_probability (float, optional): Starting value for m probability. Defaults
                to None.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            min_intersection (int, optional): The minimum cardinality of the
                intersection of arrays for this comparison level. Defaults to 1
            include_colname_in_charts_label (bool, optional): Should the charts label
                contain the column name? Defaults to False

        Examples:
            === ":simple-duckdb: DuckDB"
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.array_intersect_level("name")
                ```
            === ":simple-apachespark: Spark"
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.array_intersect_level("name")
                ```
            === ":simple-amazonaws: Athena"
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.array_intersect_level("name")
                ```
            === ":simple-postgresql: PostgreSql"
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.array_intersect_level("name")
                ```

        Returns:
            ComparisonLevel: A comparison level that evaluates the size of intersection
                of arrays
        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)

        size_array_intersection = (
            f"{self._size_array_intersect_function(col.name_l, col.name_r)}"
        )
        sql = f"{size_array_intersection} >= {min_intersection}"

        label_prefix = (
            f"{col_name} arrays" if include_colname_in_charts_label else "Arrays"
        )
        if min_intersection == 1:
            label = f"{label_prefix} intersect"
        else:
            label = f"{label_prefix} intersect size >= {min_intersection}"

        level_dict = {"sql_condition": sql, "label_for_charts": label}
        if m_probability:
            level_dict["m_probability"] = m_probability
        if term_frequency_adjustments:
            level_dict["tf_adjustment_column"] = col_name

        super().__init__(level_dict, sql_dialect=self._sql_dialect)

    @property
    def _size_array_intersect_function(self):
        raise NotImplementedError("Intersect function not defined on base class")


class DatediffLevelBase(ComparisonLevel):
    def __init__(
        self,
        date_col: str,
        date_threshold: int,
        date_metric: str = "day",
        m_probability=None,
        cast_strings_to_date=False,
        date_format=None,
    ) -> ComparisonLevel:
        """Represents a comparison level based around the difference between dates
        within a column

        Arguments:
            date_col (str): Input column name
            date_threshold (int): The total difference in time between two given
                dates. This is used in tandem with `date_metric` to determine .
                If you are using `year` as your metric, then a value of 1 would
                require that your dates lie within 1 year of one another.
            date_metric (str): The unit of time with which to measure your
                `date_threshold`.
                Your metric should be one of `day`, `month` or `year`.
                Defaults to `day`.
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.
            cast_strings_to_date (bool, optional): Set to true and adjust
                date_format param when input dates are strings to enable
                date-casting. Defaults to False.
            date_format (str, optional): Format of input dates if date-strings
                are given. Must be consistent across record pairs. If None
                (the default), downstream functions for each backend assign
                date_format to ISO 8601 format (yyyy-mm-dd).

        Examples:
            === ":simple-duckdb: DuckDB"
                Date Difference comparison level at threshold 1 year
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.datediff_level("date",
                                    date_threshold=1,
                                    date_metric="year"
                                    )
                ```
                Date Difference comparison with date-casting and unspecified
                date_format (default = %Y-%m-%d)
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True
                                    )
                ```
                Date Difference comparison with date-casting and specified date_format
                ``` python
                import splink.duckdb.comparison_level_library as cll
                cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True,
                                    date_format='%d/%m/%Y'
                                    )
                ```
            === ":simple-apachespark: Spark"
                Date Difference comparison level at threshold 1 year
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.datediff_level("date",
                                    date_threshold=1,
                                    date_metric="year"
                                    )
                ```
                Date Difference comparison with date-casting and unspecified
                date_format (default = %Y-%m-%d)
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True
                                    )
                ```
                Date Difference comparison with date-casting and specified date_format
                ``` python
                import splink.spark.comparison_level_library as cll
                cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True,
                                    date_format='%d/%m/%Y'
                                    )
                ```
            === ":simple-amazonaws: Athena"
                Date Difference comparison level at threshold 1 year
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.datediff_level("date",
                                    date_threshold=1,
                                    date_metric="year"
                                    )
                ```
                Date Difference comparison with date-casting and unspecified
                date_format (default = %Y-%m-%d)
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True
                                    )
                ```
                Date Difference comparison with date-casting and specified date_format
                ``` python
                import splink.athena.comparison_level_library as cll
                cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True,
                                    date_format='%d/%m/%Y'
                                    )
                ```
            === ":simple-postgresql: PostgreSql"
                Date Difference comparison level at threshold 1 year
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.datediff_level("date",
                                    date_threshold=1,
                                    date_metric="year"
                                    )
                ```
                Date Difference comparison with date-casting and unspecified
                date_format (default = yyyy-MM-dd)
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True
                                    )
                ```
                Date Difference comparison with date-casting and specified date_format
                ``` python
                import splink.postgres.comparison_level_library as cll
                cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True,
                                    date_format='dd/MM/yyyy'
                                    )
                ```
        Returns:
            ComparisonLevel: A comparison level that evaluates whether two dates fall
                within a given interval.
        """

        date = InputColumn(date_col, sql_dialect=self._sql_dialect)
        date_l, date_r = date.names_l_r

        datediff_sql = self._datediff_function(
            date_l,
            date_r,
            date_threshold,
            date_metric,
            cast_strings_to_date,
            date_format,
        )
        label = f"Within {date_threshold} {date_metric}"
        if date_threshold > 1:
            label += "s"

        level_dict = {
            "sql_condition": datediff_sql,
            "label_for_charts": label,
        }

        if m_probability:
            level_dict["m_probability"] = m_probability

        super().__init__(level_dict, sql_dialect=self._sql_dialect)

    @property
    def _datediff_function(self):
        raise NotImplementedError("Datediff function not defined on base class")
