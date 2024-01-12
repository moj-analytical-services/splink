from typing import List, Union

from . import comparison_level_library as cll
from .comparison_creator import ComparisonCreator
from .comparison_level_creator import ComparisonLevelCreator
from .misc import ensure_is_iterable

_fuzzy_levels = {
    "damerau_levenshtein": cll.DamerauLevenshteinLevel,
    "jaro": cll.JaroLevel,
    "jaro_winkler": cll.JaroWinklerLevel,
    "levenshtein": cll.LevenshteinLevel,
}
# metric names single quoted and comma-separated for error messages
_AVAILABLE_METRICS_STRING = ", ".join(map(lambda x: f"'{x}'", _fuzzy_levels.keys()))

_DEFAULT_EMAIL_REGEX = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+[.][a-zA-Z]{2,}$"


class EmailComparison(ComparisonCreator):

    USERNAME_REGEX = "^[^@]+"
    DOMAIN_REGEX = "@([^@]+)$"

    def __init__(
        self,
        col_name: str,
        *,
        invalid_emails_as_null: bool = False,
        valid_email_regex: str = _DEFAULT_EMAIL_REGEX,
        include_exact_match_level: bool = True,
        include_username_match_level: bool = True,
        include_username_fuzzy_level: bool = True,
        include_domain_match_level: bool = False,
        # TODO: typing.Literal? enum?
        fuzzy_metric: str = "jaro_winkler",
        thresholds: Union[float, List[float]] = [0.88],
    ):

        thresholds_as_iterable = ensure_is_iterable(thresholds)
        self.thresholds = [*thresholds_as_iterable]

        self.valid_email_regex = valid_email_regex if invalid_emails_as_null else None

        self.exact_match = include_exact_match_level
        self.username_match = include_username_match_level
        self.username_fuzzy = include_username_fuzzy_level
        self.domain_match = include_domain_match_level

        if self.thresholds:
            try:
                self.fuzzy_level = _fuzzy_levels[fuzzy_metric]
            except KeyError:
                raise ValueError(
                    f"Invalid value for {fuzzy_metric=}.  "
                    f"Must choose one of: {_AVAILABLE_METRICS_STRING}."
                ) from None
            # store metric for description
            self.fuzzy_metric = fuzzy_metric

        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        full_col_expression = self.col_expression
        username_col_expression = self.col_expression.regex_extract(self.USERNAME_REGEX)
        domain_col_expression = self.col_expression.regex_extract(self.DOMAIN_REGEX)

        levels = [
            # Null level accept pattern if not None, otherwise will ignore
            cll.NullLevel(
                full_col_expression, valid_string_pattern=self.valid_email_regex
            ),
        ]
        if self.exact_match:
            levels.append(cll.ExactMatchLevel(full_col_expression))
        if self.username_match:
            levels.append(cll.ExactMatchLevel(username_col_expression))
        if self.thresholds:
            levels.extend(
                [
                    self.fuzzy_level(full_col_expression, threshold)
                    for threshold in self.thresholds
                ]
            )
            if self.username_fuzzy:
                levels.extend(
                    [
                        self.fuzzy_level(username_col_expression, threshold)
                        for threshold in self.thresholds
                    ]
                )
        if self.domain_match:
            levels.append(cll.ExactMatchLevel(domain_col_expression))

        levels.append(cll.ElseLevel())
        return levels

    def create_description(self) -> str:
        comparison_desc = ""
        if self.exact_match:
            comparison_desc += "Exact match vs. "

        if self.username_match:
            comparison_desc += "Exact username match different domain vs. "

        if self.thresholds:
            comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
            plural = "s" if len(self.thresholds) == 1 else ""
            comparison_desc = (
                f"{self.fuzzy_metric} at threshold{plural} "
                f"{comma_separated_thresholds_string} vs. "
            )
            if self.username_fuzzy:
                comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
                plural = "s" if len(self.thresholds) == 1 else ""
                comparison_desc = (
                    f"{self.fuzzy_metric} on username at threshold{plural} "
                    f"{comma_separated_thresholds_string} vs. "
                )

        if self.domain_match:
            comparison_desc += "Domain-only match vs. "

        comparison_desc += "anything else"
        return comparison_desc

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name
