from __future__ import annotations

from .comparison_level import ComparisonLevel
from .comparison_level_sql import great_circle_distance_km_sql
from .input_column import InputColumn


class NullLevelBase(ComparisonLevel):
    def __init__(self, col_name) -> ComparisonLevel:
        """Represents comparisons level where one or both sides of the comparison
        contains null values so the similarity cannot be evaluated.
        Assumed to have a partial match weight of zero (null effect
        on overall match weight)
        Args:
            col_name (str): Input column name

        Examples:
            >>> # DuckDB Null level
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.null_level("name")

            >>> # Spark Null level
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.null_level("name")

        Returns:
            ComparisonLevel: Comparison level for null entries
        """

        col = InputColumn(col_name, sql_dialect=self._sql_dialect)
        level_dict = {
            "sql_condition": f"{col.name_l()} IS NULL OR {col.name_r()} IS NULL",
            "label_for_charts": "Null",
            "is_null_level": True,
        }
        super().__init__(level_dict, sql_dialect=self._sql_dialect)


class ExactMatchLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name,
        m_probability=None,
        term_frequency_adjustments=False,
        include_colname_in_charts_label=False,
    ) -> ComparisonLevel:
        """Represents a comparison level where there is an exact match,

        Args:
            col_name (str): Input column name
            m_probability (float, optional): Starting value for m probability
                Defaults to None.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.

        Examples:
            >>> # DuckDB Exact match level
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.exact_match_level("name")

            >>> # Spark Exact match level
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.exact_match_level("name")
        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)

        label_suffix = f" {col_name}" if include_colname_in_charts_label else ""
        level_dict = {
            "sql_condition": f"{col.name_l()} = {col.name_r()}",
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
            >>> # DuckDB Else level
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.else_level("name")

            >>> # Spark Else level
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.else_level("name")
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
        higher_is_more_similar: bool = True,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using a user-provided distance function,
        where the similarity

        Args:
            col_name (str): Input column name
            distance_function_name (str): The name of the distance function
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            higher_is_more_similar (bool): If True, a higher value of the
                distance function indicates a higher similarity (e.g. jaro_winkler).
                If false, a higher value indicates a lower similarity
                (e.g. levenshtein).
            m_probability (float, optional): Starting value for m probability
                Defaults to None.

        Returns:
            ComparisonLevel: A comparison level for a given distance function
        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)

        if higher_is_more_similar:
            operator = ">="
        else:
            operator = "<="

        sql_cond = (
            f"{distance_function_name}({col.name_l()}, {col.name_r()}) "
            f"{operator} {distance_threshold}"
        )
        chart_label = (
            f"{distance_function_name.capitalize()} {operator} {distance_threshold}"
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
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using a levenshtein distance function,

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            >>> # DuckDB Levenshtein comparison level at threshold 1
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.levenshtein_level("name", 1)

            >>> # Spark Levenshtein comparison level at thresholds 1
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.levenshtein_level("name", 1)

        Returns:
            ComparisonLevel: A comparison level that evaluates the
                levenshtein similarity
        """
        super().__init__(
            col_name,
            self._levenshtein_name,
            distance_threshold,
            False,
            m_probability=m_probability,
        )


class JaroLevelBase(DistanceFunctionLevelBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold: float,
        m_probability=None,
    ):
        """Represents a comparison using the jaro distance function

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            >>> # DuckDB Jaro comparison level at threshold 0.9
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.jaro_level("name", 0.9)

            >>> # Spark Jaro comparison level at thresholds 0.9
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.jaro_level("name", 0.9)

        Returns:
            ComparisonLevel: A comparison level that evaluates the
                jaro similarity
        """

        super().__init__(
            col_name,
            self._jaro_name,
            distance_threshold,
            True,
            m_probability=m_probability,
        )


class JaroWinklerLevelBase(DistanceFunctionLevelBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold: float,
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using the jaro winkler distance function

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            >>> # DuckDB Jaro-winkler comparison level at threshold 0.9
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.jaro_winkler_level("name", 0.9)

            >>> # Spark Jaro-winkler comparison level at threshold 0.9
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.jaro_winkler_level("name", 0.9)

        Returns:
            ComparisonLevel: A comparison level that evaluates the
                jaro winkler similarity
        """

        super().__init__(
            col_name,
            self._jaro_winkler_name,
            distance_threshold,
            True,
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
        m_probability=None,
    ) -> ComparisonLevel:
        """Represents a comparison level using a jaccard distance function

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Examples:
            >>> # DuckDB Jaccard comparison level at threshold 0.9
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.jaccard_level("name", 0.9)

            >>> # Spark Jaccard comparison level at threshold 0.9
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.jaccard_level("name", 0.9)

        Returns:
            ComparisonLevel: A comparison level that evaluates the jaccard similarity
        """
        super().__init__(
            col_name,
            self._jaccard_name,
            distance_threshold,
            True,
            m_probability=m_probability,
        )


class ColumnsReversedLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name_1: str,
        col_name_2: str,
        m_probability=None,
        tf_adjustment_column=None,
    ) -> ComparisonLevel:
        """Represents a comparison level where the columns are reversed.  For example,
        if surname is in the forename field and vice versa

        Args:
            col_name_1 (str): First column, e.g. forename
            col_name_2 (str): Second column, e.g. surname
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.
            tf_adjustment_column (str, optional): Column to use for term frequency
                adjustments if an exact match is observed. Defaults to None.

        Examples:
            >>> # DuckDB Columns Reversed comparison level
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.columns_reversed_level("first_name", "surname")

            >>> # Spark Columns Reversed comparison level
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.columns_reversed_level("first_name", "surname")


        Returns:
            ComparisonLevel: A comparison level that evaluates the exact match of two
                columns.
        """

        col_1 = InputColumn(col_name_1, sql_dialect=self._sql_dialect)
        col_2 = InputColumn(col_name_2, sql_dialect=self._sql_dialect)

        s = (
            f"{col_1.name_l()} = {col_2.name_r()} and "
            f"{col_1.name_r()} = {col_2.name_l()}"
        )
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

        Examples:
            >>> # DuckDB KM Distance comparison level at threshold 5km
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.distance_in_km_level("lat_col",
            >>>                         "long_col",
            >>>                         km_threshold=5)

            >>> # Spark KM Distance comparison level at threshold 5km
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.distance_in_km_level("lat_col",
            >>>                         "long_col",
            >>>                         km_threshold=5)

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

        Returns:
            ComparisonLevel: A comparison level that evaluates the distance between
                two coordinates
        """

        lat = InputColumn(lat_col, sql_dialect=self._sql_dialect)
        long = InputColumn(long_col, sql_dialect=self._sql_dialect)
        lat_l, lat_r = lat.names_l_r()
        long_l, long_r = long.names_l_r()

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
            >>> # DuckDB Percentage Difference comparison level with min percentage
            >>> # difference of 50%
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.percentage_difference_level("value", 0.5)

            >>> # Spark Percentage Difference comparison level with min percentage
            >>> # difference of 50%
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.percentage_difference_level("value", 0.5)

        Returns:
            ComparisonLevel: A comparison level that evaluates the percentage difference
                between two values

        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)

        s = f"""(abs({col.name_l()} - {col.name_r()})/
            (case
                when {col.name_r()} > {col.name_l()}
                then {col.name_r()}
                else {col.name_l()}
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
            >>> # DuckDB Array Intersect comparison level with min intersection size 1
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.array_intersect_level("name")

            >>> # Spark Array Intersect comparison level with min intersection size 1
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.array_intersect_level("name")

        Returns:
            ComparisonLevel: A comparison level that evaluates the size of intersection
                of arrays
        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)

        size_array_intersection = (
            f"{self._size_array_intersect_function(col.name_l(), col.name_r())}"
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


class DateDiffLevelBase(ComparisonLevel):
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
            >>> # DuckDB Date Difference comparison level at threshold 1 year
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.datediff_level("date",
            >>>                     date_threshold=1,
            >>>                     date_metric="year"
            >>>                     )

            >>> # Spark Date Difference comparison level at threshold 1 year
            >>> import splink.spark.spark_comparison_level_library as cll
            >>> cll.datediff_level("date",
            >>>                     date_threshold=1,
            >>>                     date_metric="year"
            >>>                     )

            >>> # DuckDB Date Difference comparison with date-casting and unspecified
            >>> # (default = %Y-%m-%d) date_format
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True
                                    )

            >>> # DuckDB Date Difference comparison with date-casting and specified
            >>> # date_format
            >>> import splink.duckdb.duckdb_comparison_level_library as cll
            >>> cll.datediff_level("dob",
                                    date_threshold=3,
                                    date_metric='month',
                                    cast_strings_to_date=True,
                                    date_format='%d/%m/%Y'
                                    )

        Returns:
            ComparisonLevel: A comparison level that evaluates whether two dates fall
                within a given interval.
        """

        date = InputColumn(date_col, sql_dialect=self._sql_dialect)
        date_l, date_r = date.names_l_r()

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
