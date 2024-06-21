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
    """
    A wrapper to generate a comparison for a date column the data in
    `col_name` with preselected defaults.

    The default arguments will give a comparison with comparison levels:\n
    - Exact match (1st of January only)
    - Exact match (all other dates)
    - Damerau-Levenshtein distance <= 1
    - Date difference <= 1 month
    - Date difference <= 1 year
    - Date difference <= 10 years
    - Anything else
    """

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

    def create_description(self) -> str:
        comparison_desc = "Exact match "
        if self.separate_1st_january:
            comparison_desc += "(with separate 1st Jan) "
        comparison_desc += "vs. "

        comparison_desc += "Damerau-Levenshtein distance <= 1 vs. "

        for threshold, metric in zip(self.datetime_thresholds, self.datetime_metrics):
            comparison_desc += f"{metric} difference <= {threshold} vs. "

        comparison_desc += "anything else"

        return comparison_desc

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class NameComparison(ComparisonCreator):
    """
    A wrapper to generate a comparison for a name column the data in
    `col_name` with preselected defaults.

    The default arguments will give a comparison with comparison levels:
    - Exact match
    - Jaro Winkler similarity >= 0.9
    - Jaro Winkler similarity >= 0.8
    - Anything else
    """

    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        include_exact_match_level: bool = True,
        phonetic_col_name: Union[str, ColumnExpression] = None,
        fuzzy_metric: str = "jaro_winkler",
        fuzzy_thresholds: Union[float, list[float]] = [0.9, 0.8],
    ):
        fuzzy_thresholds_as_iterable = ensure_is_iterable(fuzzy_thresholds)
        self.fuzzy_thresholds = [*fuzzy_thresholds_as_iterable]

        self.exact_match = include_exact_match_level

        if self.fuzzy_thresholds:
            try:
                self.fuzzy_level = _fuzzy_levels[fuzzy_metric]
            except KeyError:
                raise ValueError(
                    f"Invalid value for {fuzzy_metric=}.  "
                    f"Must choose one of: {_AVAILABLE_METRICS_STRING}."
                ) from None
            # store metric for description
            self.fuzzy_metric = fuzzy_metric

        cols = {"name": col_name}
        if phonetic_col_name is not None:
            cols["phonetic_name"] = phonetic_col_name
            self.phonetic_col = True
        else:
            self.phonetic_col = False
        super().__init__(cols)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        name_col_expression = self.col_expressions["name"]

        levels: list[ComparisonLevelCreator] = [
            cll.NullLevel(name_col_expression),
        ]
        if self.exact_match:
            levels.append(cll.ExactMatchLevel(name_col_expression))
        if self.phonetic_col:
            phonetic_col_expression = self.col_expressions["phonetic_name"]
            levels.append(cll.ExactMatchLevel(phonetic_col_expression))

        if self.fuzzy_thresholds:
            levels.extend(
                [
                    self.fuzzy_level(name_col_expression, threshold)
                    for threshold in self.fuzzy_thresholds
                ]
            )

        levels.append(cll.ElseLevel())
        return levels

    def create_description(self) -> str:
        comparison_desc = ""
        if self.exact_match:
            comparison_desc += "Exact match vs"
        if self.phonetic_col:
            comparison_desc += "Phonetic name match vs. "

        if self.fuzzy_thresholds:
            comma_separated_thresholds_string = ", ".join(
                map(str, self.fuzzy_thresholds)
            )
            plural = "s" if len(self.fuzzy_thresholds) > 1 else ""
            comparison_desc = (
                f"{self.fuzzy_metric} at threshold{plural} "
                f"{comma_separated_thresholds_string} vs. "
            )

        comparison_desc += "anything else"
        return comparison_desc

    def create_output_column_name(self) -> str:
        return self.col_expressions["name"].output_column_name


class ForenameSurnameComparison(ComparisonCreator):
    """
    A wrapper to generate a comparison for a name column the data in
    `col_name` with preselected defaults.

    The default arguments will give a comparison with comparison levels:\n
    - Exact match forename and surname\n
    - Macth of forename and surname reversed\n
    - Exact match surname\n
    - Exact match forename\n
    - Fuzzy match surname jaro-winkler >= 0.88\n
    - Fuzzy match forename jaro-winkler>=  0.88\n
    - Anything else
    """

    def __init__(
        self,
        forename_col_name: Union[str, ColumnExpression],
        surname_col_name: Union[str, ColumnExpression],
        *,
        include_exact_match_level: bool = True,
        include_columns_reversed: bool = True,
        # TODO: think about how this works and maybe restore?
        # tf_adjustment_col_forename_and_surname: str = None,
        phonetic_forename_col_name: Union[str, ColumnExpression] = None,
        phonetic_surname_col_name: Union[str, ColumnExpression] = None,
        fuzzy_metric: str = "jaro_winkler",
        fuzzy_thresholds: Union[float, List[float]] = [0.88],
    ):
        fuzzy_thresholds_as_iterable = ensure_is_iterable(fuzzy_thresholds)
        self.fuzzy_thresholds = [*fuzzy_thresholds_as_iterable]

        self.exact_match = include_exact_match_level
        self.columns_reversed = include_columns_reversed

        if self.fuzzy_thresholds:
            try:
                self.fuzzy_level = _fuzzy_levels[fuzzy_metric]
            except KeyError:
                raise ValueError(
                    f"Invalid value for {fuzzy_metric=}.  "
                    f"Must choose one of: {_AVAILABLE_METRICS_STRING}."
                ) from None
            # store metric for description
            self.fuzzy_metric = fuzzy_metric
        cols = {"forename": forename_col_name, "surname": surname_col_name}
        if (
            phonetic_forename_col_name is not None
            and phonetic_surname_col_name is not None
        ):
            # TODO: warn if only one set?
            self.phonetic_match = True
            cols["phonetic_forename"] = phonetic_forename_col_name
            cols["phonetic_surname"] = phonetic_surname_col_name
        else:
            self.phonetic_match = False
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
        if self.exact_match:
            levels.append(
                cll.And(
                    cll.ExactMatchLevel(forename_col_expression),
                    cll.ExactMatchLevel(surname_col_expression),
                )
            )
        if self.phonetic_match:
            phonetic_forename_col_expression = self.col_expressions["phonetic_forename"]
            phonetic_surname_col_expression = self.col_expressions["phonetic_surname"]
            levels.append(
                cll.And(
                    cll.ExactMatchLevel(phonetic_forename_col_expression),
                    cll.ExactMatchLevel(phonetic_surname_col_expression),
                )
            )
        if self.columns_reversed:
            levels.append(
                cll.ColumnsReversedLevel(
                    forename_col_expression, surname_col_expression
                )
            )

        levels.append(cll.ExactMatchLevel(surname_col_expression))
        levels.append(cll.ExactMatchLevel(forename_col_expression))

        if self.fuzzy_thresholds:
            levels.extend(
                [
                    self.fuzzy_level(surname_col_expression, threshold)
                    for threshold in self.fuzzy_thresholds
                ]
            )
            levels.extend(
                [
                    self.fuzzy_level(forename_col_expression, threshold)
                    for threshold in self.fuzzy_thresholds
                ]
            )
        levels.append(cll.ElseLevel())
        return levels

    def create_description(self) -> str:
        comparison_desc = ""
        if self.exact_match:
            comparison_desc += "Exact match both names vs. "

        if self.phonetic_match:
            comparison_desc += "Exact phonetic match both names vs. "
        if self.columns_reversed:
            comparison_desc += "Names reversed vs. "
        comparison_desc += "Surname only match vs. "
        comparison_desc += "Forename only match vs. "

        if self.fuzzy_thresholds:
            comma_separated_thresholds_string = ", ".join(
                map(str, self.fuzzy_thresholds)
            )
            plural = "s" if len(self.fuzzy_thresholds) > 1 else ""
            comparison_desc = (
                f"{self.fuzzy_metric} surname at threshold{plural} "
                f"{comma_separated_thresholds_string} vs. "
            )
            comparison_desc = (
                f"{self.fuzzy_metric} forename at threshold{plural} "
                f"{comma_separated_thresholds_string} vs. "
            )

        comparison_desc += "anything else"
        return comparison_desc

    def create_output_column_name(self) -> str:
        forename_output_name = self.col_expressions["forename"].output_column_name
        surname_output_name = self.col_expressions["surname"].output_column_name
        return f"{forename_output_name}_{surname_output_name}"


_VALID_POSTCODE_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9][A-Za-z]{2}$"


class PostcodeComparison(ComparisonCreator):
    """
    A wrapper to generate a comparison for a poscode column 'col_name'
        with preselected defaults.

    The default arguments will give a comparison with levels:\n
    - Exact match on full postcode
    - Exact match on sector
    - Exact match on district
    - Exact match on area
    - All other comparisons
    """

    SECTOR_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9]"
    DISTRICT_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]?"
    AREA_REGEX = "^[A-Za-z]{1,2}"

    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        invalid_postcodes_as_null: bool = False,
        valid_postcode_regex: str = _VALID_POSTCODE_REGEX,
        include_full_match_level: bool = True,
        include_sector_match_level: bool = True,
        include_district_match_level: bool = True,
        include_area_match_level: bool = True,
        lat_col: Union[str, ColumnExpression] = None,
        long_col: Union[str, ColumnExpression] = None,
        km_thresholds: Union[float, List[float]] = [],
    ):
        self.valid_postcode_regex = (
            valid_postcode_regex if invalid_postcodes_as_null else None
        )

        self.full_match = include_full_match_level
        self.sector_match = include_sector_match_level
        self.district_match = include_district_match_level
        self.area_match = include_area_match_level

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
        super().__init__(cols)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        full_col_expression = self.col_expressions["postcode"]
        sector_col_expression = full_col_expression.regex_extract(self.SECTOR_REGEX)
        district_col_expression = full_col_expression.regex_extract(self.DISTRICT_REGEX)
        area_col_expression = full_col_expression.regex_extract(self.AREA_REGEX)

        levels: list[ComparisonLevelCreator] = [
            # Null level accept pattern if not None, otherwise will ignore
            cll.NullLevel(
                full_col_expression, valid_string_pattern=self.valid_postcode_regex
            ),
        ]
        if self.full_match:
            levels.append(cll.ExactMatchLevel(full_col_expression))
        if self.sector_match:
            levels.append(cll.ExactMatchLevel(sector_col_expression))
        if self.district_match:
            levels.append(cll.ExactMatchLevel(district_col_expression))
        if self.area_match:
            levels.append(cll.ExactMatchLevel(area_col_expression))
        if self.km_thresholds:
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

    def create_description(self) -> str:
        comparison_desc = ""
        if self.full_match:
            comparison_desc += "Exact match vs. "

        if self.sector_match:
            comparison_desc += "Exact sector match vs. "
        if self.district_match:
            comparison_desc += "Exact district match vs. "
        if self.area_match:
            comparison_desc += "Exact area match vs. "

        if self.km_thresholds:
            comma_separated_thresholds_string = ", ".join(map(str, self.km_thresholds))
            plural = "s" if len(self.km_thresholds) > 1 else ""
            comparison_desc = (
                f"km distance within threshold{plural} "
                f"{comma_separated_thresholds_string} vs. "
            )

        comparison_desc += "anything else"
        return comparison_desc

    def create_output_column_name(self) -> str:
        return self.col_expressions["postcode"].output_column_name


class EmailComparison(ComparisonCreator):
    USERNAME_REGEX = "^[^@]+"
    DOMAIN_REGEX = "@([^@]+)$"

    def __init__(self, col_name: Union[str, ColumnExpression]):
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

    def create_description(self) -> str:
        comparison_desc = (
            "Exact match vs. "
            "Exact username match different domain vs. "
            "Jaro-Winkler at threshold 0.88 vs. "
            "Jaro-Winkler on username at threshold 0.88 vs. "
            "anything else"
        )
        return comparison_desc

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name
