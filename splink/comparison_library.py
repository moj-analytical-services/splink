from __future__ import annotations

from .comparison import Comparison
from .comparison_library_utils import (
    comparison_at_thresholds_error_logger,
    datediff_error_logger,
    distance_threshold_comparison_levels,
    distance_threshold_description,
)
from .misc import ensure_is_iterable


class ExactMatchBase(Comparison):
    def __init__(
        self,
        col_name,
        term_frequency_adjustments=False,
        m_probability_exact_match=None,
        m_probability_else=None,
        include_colname_in_charts_label=False,
    ) -> Comparison:
        """A comparison of the data in `col_name` with two levels:
        - Exact match
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            term_frequency_adjustments (bool, optional): If True, term frequency
                adjustments will be made on the exact match level. Defaults to False.
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.
            include_colname_in_charts_label: If true, append col name to label for
                charts.  Defaults to False.

        Examples:
            >>> # Simple DuckDB exact_match comparison
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.exact_match("first_name")

            >>> # Simple Spark exact_match comparison
            >>> import splink.spark.spark_comparison_library as cl
            >>> cl.exact_match("first_name")

        Returns:
            Comparison: A comparison for exact match that can be included in the Splink
                settings dictionary
        """

        comparison_dict = {
            "comparison_description": "Exact match vs. anything else",
            "comparison_levels": [
                self._null_level(col_name),
                self._exact_match_level(
                    col_name,
                    term_frequency_adjustments=term_frequency_adjustments,
                    m_probability=m_probability_exact_match,
                    include_colname_in_charts_label=include_colname_in_charts_label,
                ),
                self._else_level(m_probability=m_probability_else),
            ],
        }
        super().__init__(comparison_dict)


class DistanceFunctionAtThresholdsComparisonBase(Comparison):
    def __init__(
        self,
        col_name: str,
        distance_function_name: str,
        distance_threshold_or_thresholds: float | list,
        higher_is_more_similar: bool = True,
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        m_probability_exact_match=None,
        m_probability_or_probabilities_thres: float | list = None,
        m_probability_else=None,
    ) -> Comparison:
        """A comparison of the data in `col_name` with a user-provided distance
        function used to assess middle similarity levels.

        The user-provided distance function must exist in the SQL backend.

        An example of the output with default arguments and setting
        `distance_function_name` to `jaccard` and
        `distance_threshold_or_thresholds = [0.9,0.7]` would be

        - Exact match
        - Jaccard distance <= 0.9
        - Jaccard distance <= 0.7
        - Anything else

        Note: distance_function_at_thresholds() is primarily used in the
        backend to create the out-of-the-box cl.XXX_at_thresholds() functions

        Args:
            col_name (str): The name of the column to compare
            distance_function_name (str): The name of the distance function.
            distance_threshold_or_thresholds (Union[int, list], optional): The
                threshold(s) to use for the middle similarity level(s).
                Defaults to [1, 2].
            higher_is_more_similar (bool): If True, a higher value of the distance
                function indicates a higher similarity (e.g. jaro_winkler).
                If false, a higher value indicates a lower similarity
                (e.g. levenshtein).
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_thres (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Examples:
            >>> # DuckDB Jaccard Comparison at thresholds 0.9 and 0.7
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.distance_function_at_thresholds("name",
            >>>                    distance_function_name = 'jaccard',
            >>>                    distance_threshold_or_thresholds = [0.9, 0.7]
            >>>                    )

            >>> # Spark Jaccard Comparison at thresholds 0.9 and 0.7
            >>> import splink.spark.spark_comparison_library as cl
            >>> cl.distance_function_at_thresholds("name",
            >>>                    distance_function_name = 'jaccard',
            >>>                    distance_threshold_or_thresholds = [0.9, 0.7]
            >>>                    )


        Returns:
            Comparison: A comparison for a chosen distance function similarity that
                can be included in the Splink settings dictionary.
        """
        # Validate user inputs

        distance_threshold_or_thresholds = ensure_is_iterable(
            distance_threshold_or_thresholds
        )

        comparison_at_thresholds_error_logger(
            distance_function_name, distance_threshold_or_thresholds
        )

        comparison_levels = []
        comparison_levels.append(self._null_level(col_name))
        if include_exact_match_level:
            level = self._exact_match_level(
                col_name,
                term_frequency_adjustments=term_frequency_adjustments,
                m_probability=m_probability_exact_match,
            )
            comparison_levels.append(level)

        threshold_comparison_levels = distance_threshold_comparison_levels(
            self,
            col_name,
            distance_function_name,
            distance_threshold_or_thresholds,
            m_probability_or_probabilities_thres,
        )
        comparison_levels = comparison_levels + threshold_comparison_levels

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        # Construct comparison description
        comparison_desc = ""
        if include_exact_match_level:
            comparison_desc += "Exact match vs. "

        threshold_desc = distance_threshold_description(
            col_name, distance_function_name, distance_threshold_or_thresholds
        )
        comparison_desc += threshold_desc

        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)

    @property
    def _is_distance_subclass(self):
        return False


class LevenshteinAtThresholdsComparisonBase(DistanceFunctionAtThresholdsComparisonBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold_or_thresholds: int | list = [1, 2],
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        m_probability_exact_match=None,
        m_probability_or_probabilities_lev: float | list = None,
        m_probability_else=None,
    ) -> Comparison:
        """A comparison of the data in `col_name` with the levenshtein distance used to
        assess middle similarity levels.

        An example of the output with default arguments and setting
        `distance_threshold_or_thresholds = [1,2]` would be
        - Exact match
        - levenshtein distance <= 1
        - levenshtein distance <= 2
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            distance_threshold_or_thresholds (Union[int, list], optional): The
                threshold(s) to use for the middle similarity level(s).
                Defaults to [1, 2].
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_lev (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the thresholds specified for given function. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.
        Examples:
            >>> # DuckDB Levenshtein comparison at thresholds 1 and 2
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.levenshtein_at_thresholds("first_name", [1,2])

            >>> # Spark Levenshtein comparison at thresholds 1 and 2
            >>> import splink.spark.spark_comparison_library as cl
            >>> cl.levenshtein_at_thresholds("first_name", [1,2])

        Returns:
            Comparison: A comparison for Levenshtein similarity that can be included
                in the Splink settings dictionary.
        """

        super().__init__(
            col_name,
            self._levenshtein_name,
            distance_threshold_or_thresholds,
            False,
            include_exact_match_level,
            term_frequency_adjustments,
            m_probability_exact_match,
            m_probability_or_probabilities_lev,
            m_probability_else,
        )

    @property
    def _is_distance_subclass(self):
        return True


class JaccardAtThresholdsComparisonBase(DistanceFunctionAtThresholdsComparisonBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold_or_thresholds: int | list = [0.9, 0.7],
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        m_probability_exact_match=None,
        m_probability_or_probabilities_jac: float | list = None,
        m_probability_else=None,
    ) -> Comparison:
        """A comparison of the data in `col_name` with the jaccard distance used to
        assess middle similarity levels.

        An example of the output with default arguments and setting
        `distance_threshold_or_thresholds = [0.9,0.7]` would be
        - Exact match
        - Jaccard distance <= 0.9
        - Jaccard distance <= 0.7
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            distance_threshold_or_thresholds (Union[int, list], optional): The
                threshold(s) to use for the middle similarity level(s).
                Defaults to [0.9, 0.7].
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_jac (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the thresholds specified for given function. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Examples:
            >>> # DuckDB Jaccard comparison at thresholds 0.9 and 0.7
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.jaccard_at_thresholds("first_name", [0.9, 0.7])

            >>> # Spark Jaccard comparison at thresholds 0.9 and 0.7
            >>> import splink.spark.spark_comparison_library as cl
            >>> cl.jaccard_at_thresholds("first_name", [0.9, 0.7])

        Returns:
            Comparison: A comparison for Jaccard similarity that can be included
                in the Splink settings dictionary.
        """

        super().__init__(
            col_name,
            self._jaccard_name,
            distance_threshold_or_thresholds,
            True,
            include_exact_match_level,
            term_frequency_adjustments,
            m_probability_exact_match,
            m_probability_or_probabilities_jac,
            m_probability_else,
        )

    @property
    def _is_distance_subclass(self):
        return True


class JaroAtThresholdsComparisonBase(DistanceFunctionAtThresholdsComparisonBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold_or_thresholds: int | list = [0.9, 0.7],
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        m_probability_exact_match=None,
        m_probability_or_probabilities_jar: float | list = None,
        m_probability_else=None,
    ) -> Comparison:
        """A comparison of the data in `col_name` with the jaro distance used to
        assess middle similarity levels.

        An example of the output with default arguments and setting
        `distance_threshold_or_thresholds = [0.9, 0.7]` would be
        - Exact match
        - jaro distance <= 0.9
        - jaro distance <= 0.7
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            distance_threshold_or_thresholds (Union[int, list], optional): The
                threshold(s) to use for the middle similarity level(s).
                Defaults to [0.9, 0.7].
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_jar (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the thresholds specified for given function. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Examples:
            >>> # DuckDB Jaro comparison at thresholds 0.9 and 0.7
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.jaro_at_thresholds("first_name", [0.9, 0.7])

            >>> # Spark Jaccard comparison at thresholds 0.9 and 0.7
            >>> import splink.spark.spark_comparison_library as cl
            >>> cl.jaro_at_thresholds("first_name", [0.9, 0.7])

        Returns:
            Comparison:
        """

        super().__init__(
            col_name,
            self._jaro_name,
            distance_threshold_or_thresholds,
            True,
            include_exact_match_level,
            term_frequency_adjustments,
            m_probability_exact_match,
            m_probability_or_probabilities_jar,
            m_probability_else,
        )

    @property
    def _is_distance_subclass(self):
        return True


class JaroWinklerAtThresholdsComparisonBase(DistanceFunctionAtThresholdsComparisonBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold_or_thresholds: int | list = [0.9, 0.7],
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        m_probability_exact_match=None,
        m_probability_or_probabilities_jw: float | list = None,
        m_probability_else=None,
    ) -> Comparison:
        """A comparison of the data in `col_name` with the jaro_winkler distance used to
        assess middle similarity levels.

        An example of the output with default arguments and setting
        `distance_threshold_or_thresholds = [0.9, 0.7]` would be
        - Exact match
        - jaro_winkler distance <= 0.9
        - jaro_winkler distance <= 0.7
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            distance_threshold_or_thresholds (Union[int, list], optional): The
                threshold(s) to use for the middle similarity level(s).
                Defaults to [0.9, 0.7].
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_jw (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the thresholds specified for given function. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Examples:
            >>> # DuckDB Jaro-winkler comparison at thresholds 0.9 and 0.7
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.jaro_winkler_at_thresholds("first_name", [0.9, 0.7])

            >>> # Spark Jaro-winkler comparison at thresholds 0.9 and 0.7
            >>> import splink.spark.saprk_comparison_library as cl
            >>> cl.jaro_winkler_at_thresholds("first_name", [0.9, 0.7])

        Returns:
            Comparison: A comparison for Jaro Winkler similarity that can be included
                in the Splink settings dictionary.
        """

        super().__init__(
            col_name,
            self._jaro_winkler_name,
            distance_threshold_or_thresholds,
            True,
            include_exact_match_level,
            term_frequency_adjustments,
            m_probability_exact_match,
            m_probability_or_probabilities_jw,
            m_probability_else,
        )

    @property
    def _is_distance_subclass(self):
        return True


class ArrayIntersectAtSizesComparisonBase(Comparison):
    def __init__(
        self,
        col_name: str,
        size_or_sizes: int | list = [1],
        m_probability_or_probabilities_sizes: float | list = None,
        m_probability_else=None,
    ) -> Comparison:
        """A comparison of the data in array column `col_name` with various
        intersection sizes to assess similarity levels.

        An example of the output with default arguments and setting
        `size_or_sizes = [3, 1]` would be
        - Intersection has at least 3 elements
        - Intersection has at least 1 element (i.e. 1 or 2)
        - Anything else (i.e. empty intersection)

        Args:
            col_name (str): The name of the column to compare
            size_or_sizes (Union[int, list], optional): The size(s) of intersection
                to use for the non-'else' similarity level(s). Should be in
                descending order. Defaults to [1].
            m_probability_or_probabilities_sizes (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the sizes specified. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Examples:
            >>> # DuckDB Array intersect comparison at sizes 3 and 1
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.array_intersect_at_sizes("first_name", [3, 1])

            >>> # Spark Array intersect comparison at sizes 3 and 1
            >>> import splink.spark.spark_comparison_library as cl
            >>> cl.array_intersect_at_sizes("first_name", [3, 1])

        Returns:
            Comparison: A comparison for the intersection of arrays that can be included
                in the Splink settings dictionary.
        """

        sizes = ensure_is_iterable(size_or_sizes)
        if len(sizes) == 0:
            raise ValueError(
                "`size_or_sizes` must have at least one element, so that Comparison "
                "has more than just an 'else' level"
            )
        if any(size <= 0 for size in sizes):
            raise ValueError("All entries of `size_or_sizes` must be postive")

        if m_probability_or_probabilities_sizes is None:
            m_probability_or_probabilities_sizes = [None] * len(sizes)
        m_probabilities = ensure_is_iterable(m_probability_or_probabilities_sizes)

        comparison_levels = []
        comparison_levels.append(self._null_level(col_name))

        for size_intersect, m_prob in zip(sizes, m_probabilities):
            level = self._array_intersect_level(
                col_name, m_probability=m_prob, min_intersection=size_intersect
            )
            comparison_levels.append(level)

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        comparison_desc = ""

        size_desc = ", ".join([str(s) for s in sizes])
        plural = "" if len(sizes) == 1 else "s"
        comparison_desc += (
            f"Array intersection at minimum size{plural} {size_desc} vs. "
        )
        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)

    @property
    def _array_intersect_level(self):
        raise NotImplementedError("Intersect level not defined on base class")


class DateDiffAtThresholdsComparisonBase(Comparison):
    def __init__(
        self,
        col_name: str,
        date_thresholds: int | list = [1],
        date_metrics: str | list = ["year"],
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        m_probability_exact_match=None,
        m_probability_or_probabilities_dat: float | list = None,
        m_probability_else=None,
        cast_strings_to_date=False,
        date_format=None,
    ) -> Comparison:
        """A comparison of the data in the date column `col_name` with various
        date thresholds and metrics to assess similarity levels.

        An example of the output with default arguments and settings
        `date_thresholds = [1]` and `date_metrics = ['day']` would be
        - The two input dates are within 1 day of one another
        - Anything else (i.e. all other dates lie outside this range)

        `date_thresholds` and `date_metrics` should be used in conjunction
        with one another.
        For example, `date_thresholds = [10, 12, 15]` with
        `date_metrics = ['day', 'month', 'year']` would result in the following checks:
        - The two dates are within 10 days of one another
        - The two dates are within 12 months of one another
        - And the two dates are within 15 years of one another

        Args:
            col_name (str): The name of the date column to compare.
            date_thresholds (Union[int, list], optional): The size(s) of given date
                thresholds, to assess whether two dates fall within a given time
                interval.
                These values can be any integer value and should be used in tandem with
                `date_metrics`.
            date_metrics (Union[str, list], optional): The unit of time you wish your
                `date_thresholds` to be measured against.
                Metrics should be one of `day`, `month` or `year`.
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_dat (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the sizes specified. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.
            cast_strings_to_date (bool, optional): Set to True to
                enable date-casting when input dates are strings. Also adjust
                date_format if date-strings are not in (yyyy-mm-dd) format.
                Defaults to False.
            date_format(str, optional): Format of input dates if date-strings
                are given. Must be consistent across record pairs. If None
                (the default), downstream functions for each backend assign
                date_format to ISO 8601 format (yyyy-mm-dd).

        Examples:
            >>> # DuckDB Date Difference comparison at thresholds 10 days, 12 months
            >>> # and 15 years
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.datediff_at_thresholds("date",
            >>>                             date_thresholds = [10, 12, 15],
            >>>                             date_metrics = ['day', 'month', 'year']
            >>>                             )

            >>> # Spark Date Difference comparison at thresholds 10 days, 12 months
            >>> # and 15 years
            >>> import splink.spark.spark_comparison_library as cl
            >>> cl.datediff_at_thresholds("date",
            >>>                             date_thresholds = [10, 12, 15],
            >>>                             date_metrics = ['day', 'month', 'year']
            >>>                             )

            >>> # DuckDB datediff comparison with date-casting and unspecified
            >>> # (default = %Y-%m-%d) date_format
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.datediff_at_thresholds("dob",
                                            date_thresholds=[1,5],
                                            date_metrics = ["day", "year"],
                                            cast_strings_to_date=True
                                            )

            >>> # DuckDB datediff comparison with date-casting and specified
            >>> # (non-default) date_format
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.datediff_at_thresholds("dob",
                                            date_thresholds=[1,5],
                                            date_metrics = ["day", "year"],
                                            cast_strings_to_date=True,
                                            date_format='%d/%m/%Y'
                                            )

        Returns:
            Comparison: A comparison for Datediff that can be included in the Splink
                settings dictionary.
        """

        thresholds = ensure_is_iterable(date_thresholds)
        metrics = ensure_is_iterable(date_metrics)

        # Validate user inputs
        comparison_at_thresholds_error_logger("datediff", date_thresholds)
        datediff_error_logger(thresholds, metrics)

        if m_probability_or_probabilities_dat is None:
            m_probability_or_probabilities_dat = [None] * len(thresholds)
        m_probabilities = ensure_is_iterable(m_probability_or_probabilities_dat)

        comparison_levels = []
        comparison_levels.append(self._null_level(col_name))
        if include_exact_match_level:
            level = self._exact_match_level(
                col_name,
                term_frequency_adjustments=term_frequency_adjustments,
                m_probability=m_probability_exact_match,
            )
            comparison_levels.append(level)

        for date_thres, date_metr, m_prob in zip(thresholds, metrics, m_probabilities):
            level = self._datediff_level(
                col_name,
                date_threshold=date_thres,
                date_metric=date_metr,
                m_probability=m_prob,
                cast_strings_to_date=cast_strings_to_date,
                date_format=date_format,
            )
            comparison_levels.append(level)

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        comparison_desc = ""
        if include_exact_match_level:
            comparison_desc += "Exact match vs. "

        thres_desc = ", ".join(
            [f"{m.title()}(s): {v}" for m, v in zip(metrics, thresholds)]
        )
        plural = "" if len(thresholds) == 1 else "s"
        comparison_desc += (
            f"Dates within the following threshold{plural} {thres_desc} vs. "
        )
        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)

    @property
    def _datediff_level(self):
        raise NotImplementedError("Datediff level not defined on base class")


class DistanceInKMAtThresholdsComparisonBase(Comparison):
    def __init__(
        self,
        lat_col: str,
        long_col: str,
        km_thresholds: int | list = [0.1, 1],
        include_exact_match_level=False,
        m_probability_exact_match=None,
        m_probability_or_probabilities_km: float | list = None,
        m_probability_else=None,
    ) -> Comparison:
        """A comparison of the coordinates defined in 'lat_col' and
        'long col' giving the haversine distance between them in km.

        An example of the output with default arguments and settings
        `km_thresholds = [1]` would be
        - The two coordinates within 1 km of one another
        - Anything else (i.e.  the distance between all coordinate lie outside
        this range)

        Args:
            col_name (str): The name of the date column to compare.
            lat_col (str): The name of the column containing the lattitude of the
                coordinates.
            long_col (str): The name of the column containing the longitude of the
                coordinates.
            km_thresholds (Union[int, list], optional): The size(s) of given date
                thresholds, to assess whether two coordinates fall within a given
                distance.
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_km (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the sizes specified. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Examples:
            >>> # DuckDB KM Distance comparison at thresholds 0.1, 1, 10 kilometres
            >>> import splink.duckdb.duckdb_comparison_library as cl
            >>> cl.datediff_at_thresholds("lat_col",
            >>>                            "long_col",
            >>>                            km_thresholds = [0.1, 1, 10]
            >>>                            )

            >>> # Spark KM Distance comparison at thresholds 0.1, 1, 10 kilometres
            >>> import splink.spark.spark_comparison_library as cl
            >>> cl.datediff_at_thresholds("lat_col",
            >>>                            "long_col",
            >>>                            km_thresholds = [0.1, 1, 10]
            >>>                            )

        Returns:
            Comparison: A comparison for Distance in KM that can be included in the
                Splink settings dictionary.
        """

        thresholds = ensure_is_iterable(km_thresholds)

        if m_probability_or_probabilities_km is None:
            m_probability_or_probabilities_km = [None] * len(thresholds)
        m_probabilities = ensure_is_iterable(m_probability_or_probabilities_km)

        comparison_levels = []

        null_level = {
            "sql_condition": f"({lat_col}_l IS NULL OR {lat_col}_r IS NULL) \n"
            f"OR ({long_col}_l IS NULL OR {long_col}_r IS NULL)",
            "label_for_charts": "Null",
            "is_null_level": True,
        }
        comparison_levels.append(null_level)

        if include_exact_match_level:
            label_suffix = f" {lat_col}, {long_col}"
            level = {
                "sql_condition": f"({lat_col}_l = {lat_col}_r) \n"
                f"AND ({long_col}_l = {long_col}_r)",
                "label_for_charts": f"Exact match{label_suffix}",
            }

            if m_probability_exact_match:
                level["m_probability"] = m_probability_exact_match

            comparison_levels.append(level)

        for km_thres, m_prob in zip(km_thresholds, m_probabilities):
            level = self._distance_in_km_level(
                lat_col,
                long_col,
                km_threshold=km_thres,
                m_probability=m_prob,
            )
            comparison_levels.append(level)

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        comparison_desc = ""
        if include_exact_match_level:
            comparison_desc += "Exact match vs. "

        thres_desc = ", ".join([f"Km threshold(s): {thres}" for thres in thresholds])
        plural = "" if len(thresholds) == 1 else "s"
        comparison_desc += (
            f"Km distance within the following threshold{plural} {thres_desc} vs. "
        )
        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)
