from __future__ import annotations

from typing import List, Type, Union

from splink.internals import comparison_level_library as cll
from splink.internals.column_expression import ColumnExpression
from splink.internals.comparison_creator import ComparisonCreator
from splink.internals.comparison_level_creator import ComparisonLevelCreator
from splink.internals.comparison_level_library import DateMetricType
from splink.internals.dialects import SplinkDialect
from splink.internals.misc import ensure_is_iterable

# alternatively we could stick an inheritance layer in these, just for typing:
_fuzzy_cll_type = Union[
    Type[cll.DamerauLevenshteinLevel],
    Type[cll.JaroLevel],
    Type[cll.JaroWinklerLevel],
    Type[cll.LevenshteinLevel],
]
_fuzzy_levels: dict[str, _fuzzy_cll_type] = {
    "damerau_levenshtein": cll.DamerauLevenshteinLevel,
    "jaro": cll.JaroLevel,
    "jaro_winkler": cll.JaroWinklerLevel,
    "levenshtein": cll.LevenshteinLevel,
}
# metric names single quoted and comma-separated for error messages
_AVAILABLE_METRICS_STRING = ", ".join(map(lambda x: f"'{x}'", _fuzzy_levels.keys()))


class _DamerauLevenshteinIfSupportedElseLevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name: Union[str, ColumnExpression], distance_threshold: int):
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_threshold = distance_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        try:
            lev_fn = sql_dialect.damerau_levenshtein_function_name
        except NotImplementedError:
            lev_fn = sql_dialect.levenshtein_function_name
        return f"{lev_fn}({col.name_l}, {col.name_r}) <= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return f"Levenshtein distance of {col.label} <= {self.distance_threshold}"


class DateOfBirthComparison(ComparisonCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        input_is_string: bool,
        datetime_thresholds: Union[int, float, List[Union[int, float]]] = [1, 1, 10],
        datetime_metrics: Union[DateMetricType, List[DateMetricType]] = [
            "month",
            "year",
            "year",
        ],
        datetime_format: str = None,
        separate_1st_january: bool = False,
        invalid_dates_as_null: bool = True,
    ):
        """
        Generate an 'out of the box' comparison for a date of birth column
        in the `col_name` provided.

        Note that `input_is_string` is a required argument: you must denote whether the
        `col_name` contains if of type date/dattime or string.

        The default arguments will give a comparison with comparison levels:

        - Exact match (all other dates)
        - Damerau-Levenshtein distance <= 1
        - Date difference <= 1 month
        - Date difference <= 1 year
        - Date difference <= 10 years
        - Anything else

        Args:
            col_name (Union[str, ColumnExpression]): The column name
            input_is_string (bool): If True, the provided `col_name` must be of type
                string.  If False, it must be a date or datetime.
            datetime_thresholds (Union[int, float, List[Union[int, float]]], optional):
                Numeric thresholds for date differences. Defaults to [1, 1, 10].
            datetime_metrics (Union[DateMetricType, List[DateMetricType]], optional):
                Metrics for date differences. Defaults to ["month", "year", "year"].
            datetime_format (str, optional): The datetime format used to cast strings
                to dates.  Only used if input is a string.
            separate_1st_january (bool, optional): Used for when date of birth is
                sometimes recorded as 1st of Jan when only the year is known / If True,
                a level is included for for a  match on the year where at least one
                side of the match is a date on the the 1st of January.
            invalid_dates_as_null (bool, optional): If True, treat invalid dates as null
                as opposed to allowing e.g. an exact or levenshtein match where one side
                or both are an invalid date.  Only used if input is a string.  Defaults
                to True.
        """
        date_thresholds_as_iterable = ensure_is_iterable(datetime_thresholds)
        self.datetime_thresholds = [*date_thresholds_as_iterable]
        date_metrics_as_iterable = ensure_is_iterable(datetime_metrics)
        self.datetime_metrics = [*date_metrics_as_iterable]

        num_metrics = len(self.datetime_metrics)
        num_thresholds = len(self.datetime_thresholds)
        if num_thresholds == 0:
            raise ValueError("`date_thresholds` must have at least one entry")
        if num_metrics == 0:
            raise ValueError("`date_metrics` must have at least one entry")
        if num_metrics != num_thresholds:
            raise ValueError(
                "`date_thresholds` and `date_metrics` must have "
                "the same number of entries"
            )

        self.datetime_format = datetime_format

        self.separate_1st_january = separate_1st_january

        self.input_is_string = input_is_string
        self.invalid_dates_as_null = invalid_dates_as_null

        super().__init__(col_name)

    @property
    def datetime_parse_function(self):
        return self.col_expression.try_parse_date

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        if self.invalid_dates_as_null and self.input_is_string:
            null_col = self.datetime_parse_function(self.datetime_format)
        else:
            null_col = self.col_expression

        levels: list[ComparisonLevelCreator] = [
            cll.NullLevel(null_col),
        ]

        if self.input_is_string:
            date_as_iso_string = self.datetime_parse_function(
                self.datetime_format
            ).cast_to_string()
        else:
            date_as_iso_string = self.col_expression.cast_to_string()

        if self.separate_1st_january:
            level = cll.And(
                cll.Or(
                    cll.LiteralMatchLevel(
                        date_as_iso_string.substr(6, 5),
                        literal_value="01-01",
                        literal_datatype="string",
                        side_of_comparison="left",
                    ),
                    cll.LiteralMatchLevel(
                        date_as_iso_string.substr(6, 5),
                        literal_value="01-01",
                        literal_datatype="string",
                        side_of_comparison="right",
                    ),
                ),
                cll.ExactMatchLevel(date_as_iso_string.substr(0, 4)),
            )

            level.create_label_for_charts = (
                lambda: "Exact match on year (1st of January only)"
            )
            levels.append(level)

        levels.append(cll.ExactMatchLevel(self.col_expression))

        if self.input_is_string:
            col_expr_as_string = self.col_expression
        else:
            col_expr_as_string = self.col_expression.cast_to_string()

        levels.append(
            _DamerauLevenshteinIfSupportedElseLevenshteinLevel(
                col_expr_as_string, distance_threshold=1
            )
        )

        if self.datetime_thresholds:
            for threshold, metric in zip(
                self.datetime_thresholds, self.datetime_metrics
            ):
                levels.append(
                    cll.AbsoluteDateDifferenceLevel(
                        self.col_expression,
                        threshold=threshold,
                        metric=metric,
                        input_is_string=self.input_is_string,
                    )
                )

        levels.append(cll.ElseLevel())
        return levels

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class PostcodeComparison(ComparisonCreator):
    SECTOR_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9]"
    DISTRICT_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]?"
    AREA_REGEX = "^[A-Za-z]{1,2}"
    VALID_POSTCODE_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9][A-Za-z]{2}$"

    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        invalid_postcodes_as_null: bool = False,
        lat_col: Union[str, ColumnExpression] = None,
        long_col: Union[str, ColumnExpression] = None,
        km_thresholds: Union[float, List[float]] = [],
    ):
        """
        Generate an 'out of the box' comparison for a postcode column with the
        in the `col_name` provided.

        The default comparison levels are:

        - Null comparison
        - Exact match on full postcode
        - Exact match on sector
        - Exact match on district
        - Exact match on area
        - Distance in km (if lat_col and long_col are provided)

        It's also possible to include levels for distance in km, but this requires
        you to have geocoded your postcodes prior to importing them into Splink. Use
        the `lat_col` and `long_col` arguments to tell Splink where to find the
        latitude and longitude columns.

        See https://ideal-postcodes.co.uk/guides/uk-postcode-format
        for definitions

        Args:
            col_name (Union[str, ColumnExpression]): The column name or expression for
                the postcodes to be compared.
            invalid_postcodes_as_null (bool, optional): If True, treat invalid postcodes
                as null. Defaults to False.
            lat_col (Union[str, ColumnExpression], optional): The column name or
                expression for latitude. Required if `km_thresholds` is provided.
            long_col (Union[str, ColumnExpression], optional): The column name or
                expression for longitude. Required if `km_thresholds` is provided.
            km_thresholds (Union[float, List[float]], optional): Thresholds for distance
                in kilometers. If provided, `lat_col` and `long_col` must also be
                provided.
        """
        self.valid_postcode_regex = (
            self.VALID_POSTCODE_REGEX if invalid_postcodes_as_null else None
        )

        cols = {"postcode": col_name}
        if km_thresholds:
            km_thresholds_as_iterable = ensure_is_iterable(km_thresholds)
            self.km_thresholds = [*km_thresholds_as_iterable]
            if lat_col is None or long_col is None:
                raise ValueError(
                    "If you supply `km_thresholds` you must also provide values for "
                    "`lat_col` and `long_col`."
                )
            cols["latitude"] = lat_col
            cols["longitude"] = long_col
        else:
            self.km_thresholds = None
        super().__init__(cols)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        full_col_expression = self.col_expressions["postcode"]
        sector_col_expression = full_col_expression.regex_extract(self.SECTOR_REGEX)
        district_col_expression = full_col_expression.regex_extract(self.DISTRICT_REGEX)
        area_col_expression = full_col_expression.regex_extract(self.AREA_REGEX)

        if not self.km_thresholds:
            levels: list[ComparisonLevelCreator] = [
                cll.NullLevel(
                    full_col_expression, valid_string_pattern=self.valid_postcode_regex
                ),
                cll.ExactMatchLevel(full_col_expression),
                cll.ExactMatchLevel(sector_col_expression),
                cll.ExactMatchLevel(district_col_expression),
                cll.ExactMatchLevel(area_col_expression),
            ]
        if self.km_thresholds:
            # Don't include the very high level postcode categories
            # if using km thresholds - they are better modelled as geo distances
            levels: list[ComparisonLevelCreator] = [
                cll.NullLevel(
                    full_col_expression, valid_string_pattern=self.valid_postcode_regex
                ),
                cll.ExactMatchLevel(full_col_expression),
                cll.ExactMatchLevel(sector_col_expression),
            ]

            lat_col_expression = self.col_expressions["latitude"]
            long_col_expression = self.col_expressions["longitude"]
            levels.extend(
                [
                    cll.DistanceInKMLevel(
                        lat_col_expression, long_col_expression, km_threshold
                    )
                    for km_threshold in self.km_thresholds
                ]
            )

        levels.append(cll.ElseLevel())
        return levels

    def create_output_column_name(self) -> str:
        return self.col_expressions["postcode"].output_column_name


class EmailComparison(ComparisonCreator):
    USERNAME_REGEX = "^[^@]+"
    DOMAIN_REGEX = "@([^@]+)$"

    def __init__(self, col_name: Union[str, ColumnExpression]):
        """
        Generate an 'out of the box' comparison for an email address column with the
        in the `col_name` provided.

        The default comparison levels are:

        - Null comparison: e.g., one email is missing or invalid.
        - Exact match on full email: e.g., `john@smith.com` vs. `john@smith.com`.
        - Exact match on username part of email: e.g., `john@company.com` vs.
        `john@other.com`.
        - Jaro-Winkler similarity > 0.88 on full email: e.g., `john.smith@company.com`
        vs. `john.smyth@company.com`.
        - Jaro-Winkler similarity > 0.88 on username part of email: e.g.,
        `john.smith@company.com` vs. `john.smyth@other.com`.
        - Anything else: e.g., `john@company.com` vs. `rebecca@other.com`.

        Args:
            col_name (Union[str, ColumnExpression]): The column name or expression for
                the email addresses to be compared.
        """
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        full_col_expression = self.col_expression
        username_col_expression = full_col_expression.regex_extract(self.USERNAME_REGEX)

        levels: list[ComparisonLevelCreator] = [
            cll.NullLevel(full_col_expression, valid_string_pattern=None),
            cll.ExactMatchLevel(full_col_expression).configure(
                tf_adjustment_column=full_col_expression
            ),
            cll.ExactMatchLevel(username_col_expression).configure(
                label_for_charts="Exact match on username"
            ),
            cll.JaroWinklerLevel(full_col_expression, 0.88),
            cll.JaroWinklerLevel(username_col_expression, 0.88).configure(
                label_for_charts="Jaro-Winkler >0.88 on username"
            ),
            cll.ElseLevel(),
        ]
        return levels

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class NameComparison(ComparisonCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        jaro_winkler_thresholds: Union[float, list[float]] = [0.92, 0.88, 0.7],
        dmeta_col_name: str = None,
    ):
        """
        Generate an 'out of the box' comparison for a name column in the `col_name`
        provided.

        It's also possible to include a level for a dmetaphone match, but this requires
        you to derive a dmetaphone column prior to importing it into Splink. Note
        this is expected to be a column containing arrays of dmetaphone values, which
        are of length 1 or 2.

        The default comparison levels are:

        - Null comparison
        - Exact match
        - Jaro-Winkler similarity > 0.92
        - Jaro-Winkler similarity > 0.88
        - Jaro-Winkler similarity > 0.70
        - Anything else

        Args:
            col_name (Union[str, ColumnExpression]): The column name or expression for
                the names to be compared.
            jaro_winkler_thresholds (Union[float, list[float]], optional): Thresholds
                for Jaro-Winkler similarity. Defaults to [0.92, 0.88, 0.7].
            dmeta_col_name (str, optional): The column name for dmetaphone values.
                If provided, array intersection level is included. This column must
                contain arrays of dmetaphone values, which are of length 1 or 2.
        """

        jaro_winkler_thresholds = ensure_is_iterable(jaro_winkler_thresholds)
        self.jaro_winkler_thresholds = [*jaro_winkler_thresholds]

        cols = {"name": col_name}
        if dmeta_col_name is not None:
            cols["dmeta_name"] = dmeta_col_name
            self.dmeta_col = True
        else:
            self.dmeta_col = False

        super().__init__(cols)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        name_col_expression = self.col_expressions["name"]

        levels: list[ComparisonLevelCreator] = [
            cll.NullLevel(name_col_expression),
        ]

        exact_match = cll.ExactMatchLevel(name_col_expression)

        # Term frequencies can only be applied to 'pure' columns
        if name_col_expression.is_pure_column_or_column_reference:
            exact_match.configure(
                tf_adjustment_column=name_col_expression.raw_sql_expression
            )
        levels.append(exact_match)

        for threshold in self.jaro_winkler_thresholds:
            if threshold >= 0.88:
                levels.append(cll.JaroWinklerLevel(name_col_expression, threshold))

        if self.dmeta_col:
            dmeta_col_expression = self.col_expressions["dmeta_name"]
            levels.append(
                cll.ArrayIntersectLevel(dmeta_col_expression, min_intersection=1)
            )

        for threshold in self.jaro_winkler_thresholds:
            if threshold < 0.88:
                levels.append(cll.JaroWinklerLevel(name_col_expression, threshold))

        levels.append(cll.ElseLevel())
        return levels

    def create_output_column_name(self) -> str:
        return self.col_expressions["name"].output_column_name


class ForenameSurnameComparison(ComparisonCreator):
    def __init__(
        self,
        forename_col_name: Union[str, ColumnExpression],
        surname_col_name: Union[str, ColumnExpression],
        *,
        jaro_winkler_thresholds: Union[float, list[float]] = [0.92, 0.88],
        forename_surname_concat_col_name: str = None,
    ):
        jaro_winkler_thresholds = ensure_is_iterable(jaro_winkler_thresholds)
        self.jaro_winkler_thresholds = [*jaro_winkler_thresholds]
        cols = {"forename": forename_col_name, "surname": surname_col_name}
        if forename_surname_concat_col_name is not None:
            cols["forename_surname_concat"] = forename_surname_concat_col_name
        super().__init__(cols)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        forename_col_expression = self.col_expressions["forename"]
        surname_col_expression = self.col_expressions["surname"]

        levels: list[ComparisonLevelCreator] = [
            cll.And(
                cll.NullLevel(forename_col_expression),
                cll.NullLevel(surname_col_expression),
            )
        ]
        if "forename_surname_concat" in self.col_expressions:
            concat_col_expr = self.col_expressions["forename_surname_concat"]
            levels.append(
                cll.ExactMatchLevel(concat_col_expr).configure(
                    tf_adjustment_column=concat_col_expr.raw_sql_expression
                )
            )
        else:
            levels.append(
                cll.And(
                    cll.ExactMatchLevel(forename_col_expression),
                    cll.ExactMatchLevel(surname_col_expression),
                )
            )

        levels.append(
            cll.ColumnsReversedLevel(forename_col_expression, surname_col_expression)
        )

        for threshold in self.jaro_winkler_thresholds:
            levels.append(
                cll.And(
                    cll.JaroWinklerLevel(forename_col_expression, threshold),
                    cll.JaroWinklerLevel(surname_col_expression, threshold),
                )
            )

        levels.append(
            cll.ExactMatchLevel(surname_col_expression).configure(
                tf_adjustment_column=surname_col_expression.raw_sql_expression
            )
        )
        levels.append(
            cll.ExactMatchLevel(forename_col_expression).configure(
                tf_adjustment_column=forename_col_expression.raw_sql_expression
            )
        )

        levels.append(cll.ElseLevel())
        return levels

    def create_output_column_name(self) -> str:
        forename_output_name = self.col_expressions["forename"].output_column_name
        surname_output_name = self.col_expressions["surname"].output_column_name
        return f"{forename_output_name}_{surname_output_name}"
