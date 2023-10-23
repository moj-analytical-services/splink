# Script defining comparison templates that act as wrapper functions which produce
# comparisons based on the type of data in a column with default values to make
# it simpler to use splink out-of-the-box

from __future__ import annotations

import logging

from .comparison import Comparison  # change to self
from .comparison_level_composition import and_
from .comparison_library_utils import (
    datediff_error_logger,
    distance_threshold_comparison_levels,
    distance_threshold_description,
)
from .misc import ensure_is_iterable

logger = logging.getLogger(__name__)


class DateComparisonBase(Comparison):
    def __init__(
        self,
        col_name: str,
        cast_strings_to_date: bool = False,
        date_format: str = None,
        invalid_dates_as_null: bool = False,
        include_exact_match_level: bool = True,
        term_frequency_adjustments: bool = False,
        separate_1st_january: bool = False,
        levenshtein_thresholds: int | list = [],
        damerau_levenshtein_thresholds: int | list = [1],
        datediff_thresholds: int | list = [1, 1, 10],
        datediff_metrics: str | list = ["month", "year", "year"],
        m_probability_exact_match: float = None,
        m_probability_1st_january: float = None,
        m_probability_or_probabilities_lev: float | list = None,
        m_probability_or_probabilities_dl: float | list = None,
        m_probability_or_probabilities_datediff: float | list = None,
        m_probability_else: float = None,
    ) -> Comparison:
        """A wrapper to generate a comparison for a date column the data in
        `col_name` with preselected defaults.

        The default arguments will give a comparison with comparison levels:\n
        - Exact match (1st of January only)\n
        - Exact match (all other dates)\n
        - Damerau-Levenshtein distance <= 1\n
        - Date difference <= 1 year\n
        - Date difference <= 10 years \n
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            cast_strings_to_date (bool, optional): Set to True to
                enable date-casting when input dates are strings. Also adjust
                date_format if date-strings are not in (yyyy-mm-dd) format.
                Defaults to False.
            date_format (str, optional): Format of input dates if date-strings
                are given. Must be consistent across record pairs. If None
                (the default), downstream functions for each backend assign
                date_format to ISO 8601 format (yyyy-mm-dd).
                Set to "yyyy-MM-dd" for Spark and "%Y-%m-%d" for DuckDB
                when invalid_dates_as_null=True
            invalid_dates_as_null (bool, optional): assign any dates that do not adhere
                to date_format to the null level. Defaults to False.
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            separate_1st_january (bool, optional): If True, include a separate
                exact match comparison level when date is 1st January.
            levenshtein_thresholds (Union[int, list], optional): The thresholds to use
                for levenshtein similarity level(s).
                Defaults to []
            damerau_levenshtein_thresholds (Union[int, list], optional): The thresholds
                to use for damerau-levenshtein similarity level(s).
                Defaults to [1]
            datediff_thresholds (Union[int, list], optional): The thresholds to use
                for datediff similarity level(s).
                Defaults to [1, 1].
            datediff_metrics (Union[str, list], optional): The metrics to apply
                thresholds to for datediff similarity level(s).
                Defaults to ["month", "year"].
            cast_strings_to_date (bool, optional): Set to True to
                enable date-casting when input dates are strings. Also adjust
                date_format if date-strings are not in (yyyy-mm-dd) format.
                Defaults to False.
            date_format (str, optional): Format of input dates if date-strings
                are given. Must be consistent across record pairs. If None
                (the default), downstream functions for each backend assign
                date_format to ISO 8601 format (yyyy-mm-dd).
            m_probability_exact_match (float, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_lev (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the levenshtein thresholds specified. Defaults to None.
            m_probability_or_probabilities_dl (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the damerau-levenshtein thresholds specified. Defaults to None.
            m_probability_or_probabilities_datediff (Union[float, list], optional):
                If provided, overrides the default m probabilities
                for the datediff thresholds specified. Defaults to None.
            m_probability_else (float, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Basic Date Comparison
                ``` python
                import splink.duckdb.comparison_template_library as ctl
                ctl.date_comparison("date_of_birth")
                ```
                Bespoke Date Comparison
                ``` python
                import splink.duckdb.comparison_template_library as ctl
                ctl.date_comparison("date_of_birth",
                                    damerau_levenshtein_thresholds=[],
                                    levenshtein_thresholds=[2],
                                    datediff_thresholds=[1, 1],
                                    datediff_metrics=["month", "year"])
                ```
                Date Comparison casting columns date and assigning values that do not
                match the date_format to the null level
                ``` python
                import splink.duckdb.comparison_template_library as ctl
                ctl.date_comparison("date_of_birth",
                                    cast_strings_to_date=True,
                                    date_format='%d/%m/%Y',
                                    invalid_dates_as_null=True)
                ```
            === ":simple-apachespark: Spark"
                Basic Date Comparison
                ``` python
                import splink.spark.comparison_template_library as ctl
                ctl.date_comparison("date_of_birth")
                ```
                Bespoke Date Comparison
                ``` python
                import splink.spark.comparison_template_library as ctl
                ctl.date_comparison("date_of_birth",
                                    damerau_levenshtein_thresholds=[],
                                    levenshtein_thresholds=[2],
                                    datediff_thresholds=[1, 1],
                                    datediff_metrics=["month", "year"])
                ```
                Date Comparison casting columns date and assigning values that do not
                match the date_format to the null level
                ``` python
                import splink.spark.comparison_template_library as ctl
                ctl.date_comparison("date_of_birth",
                                    cast_strings_to_date=True,
                                    date_format='dd/mm/yyyy',
                                    invalid_dates_as_null=True)
                ```
        Returns:
            Comparison: A comparison that can be inclued in the Splink settings
                dictionary.
        """
        # Construct Comparison
        comparison_levels = []
        comparison_levels.append(
            self._null_level(
                col_name,
                invalid_dates_as_null=invalid_dates_as_null,
                valid_string_pattern=date_format,
            )
        )

        # Validate user inputs
        datediff_error_logger(thresholds=datediff_thresholds, metrics=datediff_metrics)

        if separate_1st_january:
            dob_first_jan = {
                "sql_condition": f"SUBSTR({col_name}_l, 6, 5) = '01-01'",
                "label_for_charts": "Date is 1st Jan",
            }
            comparison_level = and_(
                self._exact_match_level(col_name),
                dob_first_jan,
                label_for_charts="Exact match and 1st Jan",
            )

            if m_probability_1st_january:
                comparison_level["m_probability"] = m_probability_1st_january
            if term_frequency_adjustments:
                comparison_level["tf_adjustment_column"] = col_name
            comparison_levels.append(comparison_level)

        if include_exact_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                term_frequency_adjustments=term_frequency_adjustments,
                m_probability=m_probability_exact_match,
            )
            comparison_levels.append(comparison_level)

        levenshtein_thresholds = ensure_is_iterable(levenshtein_thresholds)
        if len(levenshtein_thresholds) > 0:
            threshold_comparison_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="levenshtein",
                distance_threshold_or_thresholds=levenshtein_thresholds,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_lev,
            )
            comparison_levels = comparison_levels + threshold_comparison_levels

        damerau_levenshtein_thresholds = ensure_is_iterable(
            damerau_levenshtein_thresholds
        )
        if len(damerau_levenshtein_thresholds) > 0:
            damerau_levenshtein_thresholds = ensure_is_iterable(
                damerau_levenshtein_thresholds
            )
            threshold_comparison_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="damerau-levenshtein",
                distance_threshold_or_thresholds=damerau_levenshtein_thresholds,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_dl,
            )
            comparison_levels = comparison_levels + threshold_comparison_levels

        datediff_thresholds = ensure_is_iterable(datediff_thresholds)
        datediff_metrics = ensure_is_iterable(datediff_metrics)
        if len(datediff_thresholds) > 0:
            if m_probability_or_probabilities_datediff is None:
                m_probability_or_probabilities_datediff = [None] * len(
                    datediff_thresholds
                )
            m_probability_or_probabilities_datediff = ensure_is_iterable(
                m_probability_or_probabilities_datediff
            )

            for thres, metric, m_prob in zip(
                datediff_thresholds,
                datediff_metrics,
                m_probability_or_probabilities_datediff,
            ):
                comparison_level = self._datediff_level(
                    col_name,
                    date_threshold=thres,
                    date_metric=metric,
                    m_probability=m_prob,
                    cast_strings_to_date=cast_strings_to_date,
                    date_format=date_format,
                )
                comparison_levels.append(comparison_level)

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        # Construct Description
        comparison_desc = ""
        if include_exact_match_level:
            comparison_desc += "Exact match vs. "

        if len(levenshtein_thresholds) > 0:
            desc = distance_threshold_description(
                col_name, "levenshtein", levenshtein_thresholds
            )
            comparison_desc += desc

        if len(damerau_levenshtein_thresholds) > 0:
            desc = distance_threshold_description(
                col_name, "damerau-levenshtein", damerau_levenshtein_thresholds
            )
            comparison_desc += desc

        if len(datediff_thresholds) > 0:
            datediff_desc = ", ".join(
                [
                    f"{m.title()}(s): {v}"
                    for v, m in zip(datediff_thresholds, datediff_metrics)
                ]
            )
            plural = "" if len(datediff_thresholds) == 1 else "s"
            comparison_desc += (
                f"Dates within the following threshold{plural} {datediff_desc} vs. "
            )

        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)

    @property
    def _is_distance_subclass(self):
        return False


class NameComparisonBase(Comparison):
    def __init__(
        self,
        col_name: str,
        regex_extract: str = None,
        set_to_lowercase: str = False,
        include_exact_match_level: bool = True,
        phonetic_col_name: str = None,
        term_frequency_adjustments: bool = False,
        levenshtein_thresholds: int | list = [],
        damerau_levenshtein_thresholds: int | list = [1],
        jaro_thresholds: float | list = [],
        jaro_winkler_thresholds: float | list = [0.9, 0.8],
        jaccard_thresholds: float | list = [],
        m_probability_exact_match_name: float = None,
        m_probability_exact_match_phonetic_name: float = None,
        m_probability_or_probabilities_lev: float | list = None,
        m_probability_or_probabilities_dl: float | list = None,
        m_probability_or_probabilities_jar: float | list = None,
        m_probability_or_probabilities_jw: float | list = None,
        m_probability_or_probabilities_jac: float | list = None,
        m_probability_else: float = None,
    ) -> Comparison:
        """A wrapper to generate a comparison for a name column the data in
        `col_name` with preselected defaults.

        The default arguments will give a comparison with comparison levels:\n
        - Exact match \n
        - Damerau-Levenshtein Distance <= 1
        - Jaro Winkler similarity >= 0.9\n
        - Jaro Winkler similarity >= 0.8\n
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            regex_extract (str): Regular expression pattern to evaluate a match on.
            set_to_lowercase (bool): If True, all names are set to lowercase
                during the pairwise comparisons.
                Defaults to False
            include_exact_match_level (bool, optional): If True, include an exact match
                level for col_name. Defaults to True.
            phonetic_col_name (str): The name of the column with phonetic reduction
                (such as dmetaphone) of col_name. Including parameter will create
                an exact match level for  phonetic_col_name. The phonetic column must
                be present in the dataset to use this parameter.
                Defaults to None
            term_frequency_adjustments (bool, optional): If True, apply term
                frequency adjustments to the exact match level for "col_name".
                Defaults to False.
            term_frequency_adjustments_phonetic_name (bool, optional): If True, apply
                term frequency adjustments to the exact match level for
                "phonetic_col_name".
                Defaults to False.
            levenshtein_thresholds (Union[int, list], optional): The thresholds to use
                for levenshtein similarity level(s).
                Defaults to []
            damerau_levenshtein_thresholds (Union[int, list], optional): The thresholds
                to use for damerau-levenshtein similarity level(s).
                Defaults to [1]
            jaro_thresholds (Union[int, list], optional): The thresholds to use
                for jaro similarity level(s).
                Defaults to []
            jaro_winkler_thresholds (Union[int, list], optional): The thresholds to use
                for jaro_winkler similarity level(s).
                Defaults to [0.9, 0.8]
            jaccard_thresholds (Union[int, list], optional): The thresholds to use
                for jaccard similarity level(s).
                Defaults to []
            m_probability_exact_match_name (_type_, optional): Starting m probability
                for exact match level. Defaults to None.
            m_probability_exact_match_phonetic_name (_type_, optional): Starting m
                probability for exact match level for phonetic_col_name.
                Defaults to None.
            m_probability_or_probabilities_lev (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_dl (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_datediff (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_jar (Union[float, list], optional):
                Starting m probabilities for the jaro thresholds specified.
                Defaults to None.
            m_probability_or_probabilities_jw (Union[float, list], optional):
                Starting m probabilities for the jaro winkler thresholds specified.
                Defaults to None.
            m_probability_or_probabilities_jac (Union[float, list], optional):
                Starting m probabilities for the jaccard thresholds specified.
                Defaults to None.
            m_probability_else (_type_, optional): Starting m probability for
                the 'everything else' level. Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Basic Name Comparison
                ``` python
                import splink.duckdb.comparison_template_library as ctl
                ctl.name_comparison("name")
                ```
                Bespoke Name Comparison
                ``` python
                import splink.duckdb.comparison_template_library as ctl
                ctl.name_comparison("name",
                                    phonetic_col_name = "name_dm",
                                    term_frequency_adjustments = True,
                                    levenshtein_thresholds=[2],
                                    damerau_levenshtein_thresholds=[],
                                    jaro_winkler_thresholds=[],
                                    jaccard_thresholds=[1]
                                    )
                ```
            === ":simple-apachespark: Spark"
                Basic Name Comparison
                ``` python
                import splink.spark.comparison_template_library as ctl
                ctl.name_comparison("name")
                ```
                Bespoke Name Comparison
                ``` python
                import splink.spark.comparison_template_library as ctl
                ctl.name_comparison("name",
                                    phonetic_col_name = "name_dm",
                                    term_frequency_adjustments = True,
                                    levenshtein_thresholds=[2],
                                    damerau_levenshtein_thresholds=[],
                                    jaro_winkler_thresholds=[],
                                    jaccard_thresholds=[1]
                                    )
                ```
            === ":simple-sqlite: SQLite"
                Basic Name Comparison
                ``` python
                import splink.sqlite.comparison_template_library as ctl
                ctl.name_comparison("name")
                ```
                Bespoke Name Comparison
                ``` python
                import splink.sqlite.comparison_template_library as ctl
                ctl.name_comparison("name",
                                    phonetic_col_name = "name_dm",
                                    term_frequency_adjustments = True,
                                    levenshtein_thresholds=[2],
                                    damerau_levenshtein_thresholds=[],
                                    jaro_winkler_thresholds=[0.8],
                                    )
                ```

        Returns:
            Comparison: A comparison that can be included in the Splink settings
                dictionary.
        """

        # Construct Comparison
        comparison_levels = []
        comparison_levels.append(self._null_level(col_name))

        if include_exact_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                term_frequency_adjustments=term_frequency_adjustments,
                m_probability=m_probability_exact_match_name,
                include_colname_in_charts_label=True,
                regex_extract=regex_extract,
                set_to_lowercase=set_to_lowercase,
            )
            comparison_levels.append(comparison_level)

            if phonetic_col_name is not None:
                comparison_level = self._exact_match_level(
                    phonetic_col_name,
                    term_frequency_adjustments=term_frequency_adjustments,
                    m_probability=m_probability_exact_match_phonetic_name,
                    include_colname_in_charts_label=True,
                    regex_extract=regex_extract,
                    set_to_lowercase=set_to_lowercase,
                )
                comparison_levels.append(comparison_level)

        levenshtein_thresholds = ensure_is_iterable(levenshtein_thresholds)
        if len(levenshtein_thresholds) > 0:
            threshold_comparison_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="levenshtein",
                distance_threshold_or_thresholds=levenshtein_thresholds,
                regex_extract=regex_extract,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_lev,
            )
            comparison_levels = comparison_levels + threshold_comparison_levels

        damerau_levenshtein_thresholds = ensure_is_iterable(
            damerau_levenshtein_thresholds
        )
        if len(damerau_levenshtein_thresholds) > 0:
            levenshtein_thresholds = ensure_is_iterable(damerau_levenshtein_thresholds)
            threshold_comparison_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="damerau-levenshtein",
                distance_threshold_or_thresholds=damerau_levenshtein_thresholds,
                regex_extract=regex_extract,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_dl,
            )
            comparison_levels = comparison_levels + threshold_comparison_levels

        jaro_thresholds = ensure_is_iterable(jaro_thresholds)
        if len(jaro_thresholds) > 0:
            threshold_comparison_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="jaro",
                distance_threshold_or_thresholds=jaro_thresholds,
                regex_extract=regex_extract,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_jar,
            )
            comparison_levels = comparison_levels + threshold_comparison_levels

        jaro_winkler_thresholds = ensure_is_iterable(jaro_winkler_thresholds)
        if len(jaro_winkler_thresholds) > 0:
            threshold_comparison_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="jaro-winkler",
                distance_threshold_or_thresholds=jaro_winkler_thresholds,
                regex_extract=regex_extract,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_jw,
            )
            comparison_levels = comparison_levels + threshold_comparison_levels

        jaccard_thresholds = ensure_is_iterable(jaccard_thresholds)
        if len(jaccard_thresholds) > 0:
            threshold_comparison_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="jaccard",
                distance_threshold_or_thresholds=jaccard_thresholds,
                regex_extract=regex_extract,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_jac,
            )
            comparison_levels = comparison_levels + threshold_comparison_levels

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        # Construct Description
        comparison_desc = ""
        if include_exact_match_level:
            comparison_desc += "Exact match vs. "

        if phonetic_col_name is not None:
            comparison_desc += "Names with phonetic exact match vs. "

        if len(levenshtein_thresholds) > 0:
            desc = distance_threshold_description(
                col_name, "levenshtein", levenshtein_thresholds
            )
            comparison_desc += desc

        if len(damerau_levenshtein_thresholds) > 0:
            desc = distance_threshold_description(
                col_name, "damerau-levenshtein", damerau_levenshtein_thresholds
            )
            comparison_desc += desc

        if len(jaro_thresholds) > 0:
            desc = distance_threshold_description(col_name, "jaro", jaro_thresholds)
            comparison_desc += desc

        if len(jaro_winkler_thresholds) > 0:
            desc = distance_threshold_description(
                col_name, "jaro_winkler", jaro_winkler_thresholds
            )
            comparison_desc += desc

        if len(jaccard_thresholds) > 0:
            desc = distance_threshold_description(
                col_name, "jaccard", jaccard_thresholds
            )
            comparison_desc += desc

        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)

    @property
    def _is_distance_subclass(self):
        return False


class ForenameSurnameComparisonBase(Comparison):
    def __init__(
        self,
        forename_col_name,
        surname_col_name,
        set_to_lowercase=False,
        include_exact_match_level: bool = True,
        include_columns_reversed: bool = True,
        term_frequency_adjustments: bool = False,
        tf_adjustment_col_forename_and_surname: str = None,
        phonetic_forename_col_name: str = None,
        phonetic_surname_col_name: str = None,
        levenshtein_thresholds: int | list = [],
        damerau_levenshtein_thresholds: int | list = [],
        jaro_winkler_thresholds: float | list = [0.88],
        jaro_thresholds: float | list = [],
        jaccard_thresholds: float | list = [],
        m_probability_exact_match_forename_surname: float = None,
        m_probability_exact_match_phonetic_forename_surname: float = None,
        m_probability_columns_reversed_forename_surname: float = None,
        m_probability_exact_match_surname: float = None,
        m_probability_exact_match_forename: float = None,
        m_probability_or_probabilities_surname_lev: float | list = None,
        m_probability_or_probabilities_surname_dl: float | list = None,
        m_probability_or_probabilities_surname_jw: float | list = None,
        m_probability_or_probabilities_surname_jar: float | list = None,
        m_probability_or_probabilities_surname_jac: float | list = None,
        m_probability_or_probabilities_forename_lev: float | list = None,
        m_probability_or_probabilities_forename_dl: float | list = None,
        m_probability_or_probabilities_forename_jw: float | list = None,
        m_probability_or_probabilities_forename_jar: float | list = None,
        m_probability_or_probabilities_forename_jac: float | list = None,
        m_probability_else: float = None,
    ) -> Comparison:
        """A wrapper to generate a comparison for a name column the data in
        `col_name` with preselected defaults.

        The default arguments will give a comparison with comparison levels:\n
        - Exact match forename and surname\n
        - Macth of forename and surname reversed\n
        - Exact match surname\n
        - Exact match forename\n
        - Fuzzy match surname jaro-winkler >= 0.88\n
        - Fuzzy match forename jaro-winkler>=  0.88\n
        - Anything else

        Args:
            forename_col_name (str): The name of the forename column to compare
            surname_col_name (str): The name of the surname column to compare
            set_to_lowercase (bool): If True, all names are set to lowercase
                during the pairwise comparisons.
                Defaults to False
            include_exact_match_level (bool, optional): If True, include an exact match
                level for col_name. Defaults to True.
            include_columns_reversed (bool, optional): If True, include a comparison
                level for forename and surname being swapped. Defaults to True
            term_frequency_adjustments (bool, optional): If True, apply term
                frequency adjustments to the exact match level for forename_col_name
                and surname_col_name.
                Applies term frequency adjustments to full name exact match level
                and columns reversed exact match level if
                tf_adjustment_col_forename_and_surname is provided.
                Applies term frequency adjustments to phonetic_forename_col_name
                and phonetic_surname_col_name exact match levels, if they are provided.
                Defaults to False.
            tf_adjustment_col_forename_and_surname (str, optional): The name
                of a combined forename surname column. This column is used to provide
                term frequency adjustments for forename surname exact match and columns
                reversed levels.
                Defaults to None
            set_to_lowercase (bool): If True, all postcodes are set to lowercase
                during the pairwise comparisons.
                Defaults to True
            phonetic_forename_col_name (str, optional): The name of the column with
                phonetic reduction (such as dmetaphone) of forename_col_name. Including
                parameter along with phonetic_surname_col_name will create an exact
                match level for "Full name phonetic match".
                The phonetic column must be present in the dataset to use this
                parameter.
                Defaults to None
            phonetic_surname_col_name (str, optional): The name of the column with
                phonetic reduction (such as dmetaphone) of surname_col_name. Including
                this parameter along with phonetic_forename_col_name will create an
                exact match level for "Full name phonetic match".
                The phonetic column must be present in
                the dataset to use this parameter.
                Defaults to None
            levenshtein_thresholds (Union[int, list], optional): The thresholds
                to use for levenshtein similarity level(s) for surname_col_name
                and forename_col_name.
                Defaults to []
            damerau_levenshtein_thresholds (Union[int, list], optional): The thresholds
                to use for damerau-levenshtein similarity level(s).
                Defaults to []
            jaro_winkler_thresholds (Union[int, list], optional): The thresholds
                to use for jaro_winkler similarity level(s) for surname_col_name
                and forename_col_name.
                Defaults to [0.88]
            jaro_thresholds (Union[int, list], optional): The thresholds
                to use for jaro similarity level(s) for surname_col_name
                and forename_col_name.
                Defaults to []
            jaccard_thresholds (Union[int, list], optional): The thresholds to
                use for jaccard similarity level(s) for surname_col_name and
                forename_col_name.
                Defaults to []
            m_probability_exact_match_forename_surname (_type_, optional): If provided,
                overrides the default m probability for the exact match level for
                forename and surname.
                Defaults to None.
            m_probability_exact_match_phonetic_forename_surname (_type_, optional): If
                provided, overrides the default m probability for the phonetic match
                level for forename and surname.
                Defaults to None.
            m_probability_columns_reversed_forename_surname (_type_, optional): If
                provided, overrides the default m probability for the columns reversed
                level for forename and surname.
                Defaults to None.
            m_probability_columns_reversed_forename_surname (_type_, optional): If
                provided, overrides the default m probability for the columns reversed
                level for forename and surname.
                Defaults to None.
            m_probability_exact_match_surname (_type_, optional): If provided,
                overrides the default m probability for the surname exact match
                level for forename and surname.
                Defaults to None.
            m_probability_exact_match_forename (_type_, optional): If provided,
                overrides the default m probability for the forename exact match
                level for forename and forename.
                Defaults to None.
            m_probability_phonetic_match_surname (_type_, optional): If provided,
                overrides the default m probability for the surname phonetic match
                level for forename and surname.
                Defaults to None.
            m_probability_phonetic_match_forename (_type_, optional): If provided,
                overrides the default m probability for the forename phonetic match
                level for forename and forename.
                Defaults to None.
            m_probability_or_probabilities_surname_lev (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_surname_dl (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_surname_jw (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_surname_jar (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_surname_jac (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_forename_lev (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_forename_dl (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_forename_jw (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_forename_jar (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_forename_jac (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Basic Forename Surname Comparison
                ```py
                import splink.duckdb.comparison_template_library as ctl
                ctl.forename_surname_comparison("first_name", "surname)
                ```

                Bespoke Forename Surname Comparison
                ```py
                import splink.duckdb.comparison_template_library as ctl
                ctl.forename_surname_comparison(
                        "forename",
                        "surname",
                        term_frequency_adjustments=True,
                        tf_adjustment_col_forename_and_surname="full_name",
                        phonetic_forename_col_name="forename_dm",
                        phonetic_surname_col_name="surname_dm",
                        levenshtein_thresholds=[2],
                        jaro_winkler_thresholds=[],
                        jaccard_thresholds=[1],
                    )
                ```
            === ":simple-apachespark: Spark"
                Basic Forename Surname Comparison
                ```py
                import splink.spark.comparison_template_library as ctl
                ctl.forename_surname_comparison("first_name", "surname)
                ```

                Bespoke Forename Surname Comparison
                ```py
                import splink.spark.comparison_template_library as ctl
                ctl.forename_surname_comparison(
                        "forename",
                        "surname",
                        term_frequency_adjustments=True,
                        tf_adjustment_col_forename_and_surname="full_name",
                        phonetic_forename_col_name="forename_dm",
                        phonetic_surname_col_name="surname_dm",
                        levenshtein_thresholds=[2],
                        jaro_winkler_thresholds=[],
                        jaccard_thresholds=[1],
                    )
                ```
            === ":simple-sqlite: SQLite"
                Basic Forename Surname Comparison
                ```py
                import splink.sqlite.comparison_template_library as ctl
                ctl.forename_surname_comparison("first_name", "surname)
                ```

                Bespoke Forename Surname Comparison
                ```py
                import splink.sqlite.comparison_template_library as ctl
                ctl.forename_surname_comparison(
                        "forename",
                        "surname",
                        term_frequency_adjustments=True,
                        tf_adjustment_col_forename_and_surname="full_name",
                        phonetic_forename_col_name="forename_dm",
                        phonetic_surname_col_name="surname_dm",
                        levenshtein_thresholds=[2],
                        jaro_winkler_thresholds=[0.8],
                    )
                ```


        Returns:
            Comparison: A comparison that can be included in the Splink settings
                dictionary.
        """

        # Construct Comparison
        comparison_levels = []

        comparison_level = and_(
            self._null_level(forename_col_name),
            self._null_level(surname_col_name),
            label_for_charts="Null",
        )

        comparison_levels.append(comparison_level)

        ### Forename surname exact match

        if include_exact_match_level:
            if set_to_lowercase:
                forename_col_name_l = f"lower({forename_col_name}_l)"
                forename_col_name_r = f"lower({forename_col_name}_r)"
                surname_col_name_l = f"lower({surname_col_name}_l)"
                surname_col_name_r = f"lower({surname_col_name}_r)"
            else:
                forename_col_name_l = f"{forename_col_name}_l"
                forename_col_name_r = f"{forename_col_name}_r"
                surname_col_name_l = f"{surname_col_name}_l"
                surname_col_name_r = f"{surname_col_name}_r"

            comparison_level = {
                "sql_condition": f"{forename_col_name_l} = {forename_col_name_r} "
                f"AND {surname_col_name_l} = {surname_col_name_r}",
                "tf_adjustment_column": tf_adjustment_col_forename_and_surname,
                "tf_adjustment_weight": 1.0,
                "m_probability": m_probability_exact_match_forename_surname,
                "label_for_charts": "Full name exact match",
            }

            comparison_levels.append(comparison_level)

        ### Phonetic forename surname match

        if phonetic_forename_col_name and phonetic_surname_col_name is not None:
            comparison_level = {
                "sql_condition": f"{phonetic_forename_col_name}_l = "
                f"{phonetic_forename_col_name}_r"
                f" AND {phonetic_surname_col_name}_l = {phonetic_surname_col_name}_r",
                "tf_adjustment_column": tf_adjustment_col_forename_and_surname,
                "tf_adjustment_weight": 1.0,
                "m_probability": m_probability_exact_match_phonetic_forename_surname,
                "label_for_charts": "Full name phonetic match",
            }
            comparison_levels.append(comparison_level)

        ### Columns reversed match

        if include_columns_reversed:
            comparison_level = self._columns_reversed_level(
                forename_col_name,
                surname_col_name,
                set_to_lowercase=set_to_lowercase,
                tf_adjustment_column=tf_adjustment_col_forename_and_surname,
                m_probability=m_probability_columns_reversed_forename_surname,
            )
            comparison_levels.append(comparison_level)

        ### Surname Exact match

        comparison_level = self._exact_match_level(
            surname_col_name,
            set_to_lowercase=set_to_lowercase,
            term_frequency_adjustments=term_frequency_adjustments,
            m_probability=m_probability_exact_match_surname,
            include_colname_in_charts_label=True,
        )
        comparison_levels.append(comparison_level)

        ### Forename Exact match

        comparison_level = self._exact_match_level(
            forename_col_name,
            set_to_lowercase=set_to_lowercase,
            term_frequency_adjustments=term_frequency_adjustments,
            m_probability=m_probability_exact_match_forename,
            include_colname_in_charts_label=True,
        )
        comparison_levels.append(comparison_level)

        ### Ensure fuzzy match thresholds are iterable
        levenshtein_thresholds = ensure_is_iterable(levenshtein_thresholds)
        damerau_levenshtein_thresholds = ensure_is_iterable(
            damerau_levenshtein_thresholds
        )
        jaro_thresholds = ensure_is_iterable(jaro_thresholds)
        jaro_winkler_thresholds = ensure_is_iterable(jaro_winkler_thresholds)
        jaccard_thresholds = ensure_is_iterable(jaccard_thresholds)

        ### Surname Fuzzy match
        if len(levenshtein_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                surname_col_name,
                distance_function_name="levenshtein",
                distance_threshold_or_thresholds=levenshtein_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_surname_lev,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(damerau_levenshtein_thresholds) > 0:
            levenshtein_thresholds = ensure_is_iterable(damerau_levenshtein_thresholds)
            threshold_comparison_levels = distance_threshold_comparison_levels(
                self,
                surname_col_name,
                distance_function_name="damerau-levenshtein",
                distance_threshold_or_thresholds=damerau_levenshtein_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_surname_dl,
            )
            comparison_levels = comparison_levels + threshold_comparison_levels

        if len(jaro_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                surname_col_name,
                distance_function_name="jaro-",
                distance_threshold_or_thresholds=jaro_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_surname_jar,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(jaro_winkler_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                surname_col_name,
                distance_function_name="jaro-winkler",
                distance_threshold_or_thresholds=jaro_winkler_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_surname_jw,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(jaccard_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                surname_col_name,
                distance_function_name="jaccard",
                distance_threshold_or_thresholds=jaccard_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_surname_jac,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        ### Forename Fuzzy match

        if len(levenshtein_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                forename_col_name,
                distance_function_name="levenshtein",
                distance_threshold_or_thresholds=levenshtein_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_forename_lev,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(damerau_levenshtein_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                forename_col_name,
                distance_function_name="damerau-levenshtein",
                distance_threshold_or_thresholds=damerau_levenshtein_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_forename_dl,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(jaro_winkler_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                forename_col_name,
                distance_function_name="jaro-winkler",
                distance_threshold_or_thresholds=jaro_winkler_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_forename_jw,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(jaro_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                forename_col_name,
                distance_function_name="jaro",
                distance_threshold_or_thresholds=jaro_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_forename_jar,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(jaccard_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                forename_col_name,
                distance_function_name="jaccard",
                distance_threshold_or_thresholds=jaccard_thresholds,
                set_to_lowercase=set_to_lowercase,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_forename_jac,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        # Construct Description
        comparison_desc = ""
        if include_exact_match_level:
            comparison_desc += "Exact match vs. "

        if phonetic_forename_col_name and phonetic_surname_col_name is not None:
            comparison_desc += "Phonetic match forename and surname vs. "

        if include_columns_reversed:
            comparison_desc += "Forename and surname columns reversed vs. "

        comparison_desc += "Surname exact match vs. "

        comparison_desc += "Forename exact match vs. "

        if len(levenshtein_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                surname_col_name, "levenshtein", levenshtein_thresholds
            )

        if len(damerau_levenshtein_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                surname_col_name, "damerau-levenshtein", damerau_levenshtein_thresholds
            )

        if len(jaro_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                surname_col_name, "jaro", jaro_thresholds
            )

        if len(jaro_winkler_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                surname_col_name, "jaro-winkler", jaro_winkler_thresholds
            )

        if len(jaccard_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                surname_col_name, "jaccard", jaccard_thresholds
            )

        if len(levenshtein_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                forename_col_name, "levenshtein", levenshtein_thresholds
            )

        if len(damerau_levenshtein_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                surname_col_name, "damerau-levenshtein", damerau_levenshtein_thresholds
            )

        if len(jaro_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                forename_col_name, "jaro", jaro_thresholds
            )

        if len(jaro_winkler_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                forename_col_name, "jaro-winkler", jaro_winkler_thresholds
            )

        if len(jaccard_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                forename_col_name, "jaccard", jaccard_thresholds
            )

        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)

    @property
    def _is_distance_subclass(self):
        return False


class PostcodeComparisonBase(Comparison):
    def __init__(
        self,
        col_name: str,
        invalid_postcodes_as_null=False,
        set_to_lowercase=True,
        valid_postcode_regex="^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9][A-Za-z]{2}$",
        term_frequency_adjustments_full=False,
        include_full_match_level=True,
        include_sector_match_level=True,
        include_district_match_level=True,
        include_area_match_level=True,
        lat_col: str = None,
        long_col: str = None,
        km_thresholds: int | float | list = [],
        m_probability_full_match=None,
        m_probability_sector_match=None,
        m_probability_district_match=None,
        m_probability_area_match=None,
        m_probability_or_probabilities_km_distance=None,
        m_probability_else=None,
    ) -> Comparison:
        """A wrapper to generate a comparison for a poscode column 'col_name'
            with preselected defaults.

        The default arguments will give a comparison with levels:\n
        - Exact match on full postcode\n
        - Exact match on sector\n
        - Exact match on district\n
        - Exact match on area\n
        - All other comparisons

        Args:
            col_name (str): The name of the column to compare.
            invalid_postcodes_as_null (bool): If True, postcodes that do not adhere
                to valid_postcode_regex will be included in the null level.
                Defaults to False
            set_to_lowercase (bool): If True, all postcodes are set to lowercase
                during the pairwise comparisons.
                Defaults to True
            valid_postcode_regex (str): regular expression pattern that is used
                to validate postcodes. If invalid_postcodes_as_null is True,
                postcodes that do not adhere to valid_postcode_regex will be included
                 in the null level.
                 Defaults to "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9][A-Za-z]{2}$"
            term_frequency_adjustments_full (bool, optional): If True, apply
                term frequency adjustments to the full postcode exact match level.
                Defaults to False.
            include_full_match_level (bool, optional): If True, include an exact
                match on full postcode level. Defaults to True.
            include_sector_match_level (bool, optional): If True, include an exact
                match on sector level. Defaults to True.
            include_district_match_level (bool, optional): If True, include an exact
                match on district level. Defaults to True.
            include_area_match_level (bool, optional): If True, include an exact
                match on area level. Defaults to True.
            include_distance_in_km_level (bool, optional): If True, include a
                comparison of distance between postcodes as measured in kilometers.
                Defaults to False.
            lat_col (str): The name of a latitude column or the respective array
                or struct column column containing the information, plus an index.
                For example: lat, long_lat['lat'] or long_lat[0].
            long_col (str): The name of a longitudinal column or the respective array
                or struct column column containing the information, plus an index.
                For example: long, long_lat['long'] or long_lat[1].
            km_thresholds (int, float, list): The total distance in kilometers to
                evaluate the distance_in_km_level comparison against.
            m_probability_full_match (float, optional): Starting m
                probability for full match level. Defaults to None.
            m_probability_sector_match (float, optional): Starting m
                probability for sector match level. Defaults to None.
            m_probability_district_match (float, optional): Starting m
                probability for district match level. Defaults to None.
            m_probability_area_match (float, optional): Starting m
                probability for area match level. Defaults to None.
            m_probability_or_probabilities_km_distance (float, optional): Starting m
                probability for 'distance in km' level. Defaults to None.
            m_probability_else (float, optional): Starting m probability for
                the 'everything else' level. Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Basic Postcode Comparison
                ``` python
                import splink.duckdb.comparison_template_library as ctl
                ctl.postcode_comparison("postcode")
                ```
                Bespoke Postcode Comparison
                ``` python
                import splink.duckdb.comparison_template_library as ctl
                ctl.postcode_comparison("postcode",
                                    invalid_postcodes_as_null=True,
                                    include_distance_in_km_level=True,
                                    lat_col="lat",
                                    long_col="long",
                                    km_thresholds=[10, 100]
                                    )
                ```
            === ":simple-apachespark: Spark"
                Basic Postcode Comparison
                ``` python
                import splink.spark.comparison_template_library as ctl
                ctl.postcode_comparison("postcode")
                ```
                Bespoke Postcode Comparison
                ``` python
                import splink.spark.comparison_template_library as ctl
                ctl.postcode_comparison("postcode",
                                    invalid_postcodes_as_null=True,
                                    include_distance_in_km_level=True,
                                    lat_col="lat",
                                    long_col="long",
                                    km_thresholds=[10, 100]
                                    )
                ```
            === ":simple-amazonaws: Athena"
                Basic Postcode Comparison
                ``` python
                import splink.athena.comparison_template_library as ctl
                ctl.postcode_comparison("postcode")
                ```
                Bespoke Postcode Comparison
                ``` python
                import splink.athena.comparison_template_library as ctl
                ctl.postcode_comparison("postcode",
                                    invalid_postcodes_as_null=True,
                                    include_distance_in_km_level=True,
                                    lat_col="lat",
                                    long_col="long",
                                    km_thresholds=[10, 100]
                                    )
                ```

        Returns:
            Comparison: A comparison that can be inclued in the Splink settings
                dictionary.
        """

        comparison_levels = []

        if invalid_postcodes_as_null:
            comparison_levels.append(self._null_level(col_name, valid_postcode_regex))
        else:
            comparison_levels.append(self._null_level(col_name))

        if include_full_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                regex_extract=None,
                term_frequency_adjustments=term_frequency_adjustments_full,
                set_to_lowercase=set_to_lowercase,
                m_probability=m_probability_full_match,
                include_colname_in_charts_label=True,
            )
            comparison_levels.append(comparison_level)

        if include_sector_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                regex_extract="^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9]",
                set_to_lowercase=set_to_lowercase,
                m_probability=m_probability_sector_match,
                manual_col_name_for_charts_label="Postcode Sector",
            )
            comparison_levels.append(comparison_level)

        if include_district_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                regex_extract="^[A-Za-z]{1,2}[0-9][A-Za-z0-9]?",
                set_to_lowercase=set_to_lowercase,
                m_probability=m_probability_district_match,
                manual_col_name_for_charts_label="Postcode District",
            )
            comparison_levels.append(comparison_level)

        if include_area_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                regex_extract="^[A-Za-z]{1,2}",
                set_to_lowercase=set_to_lowercase,
                m_probability=m_probability_area_match,
                manual_col_name_for_charts_label="Postcode Area",
            )
            comparison_levels.append(comparison_level)

        km_thresholds = ensure_is_iterable(km_thresholds)
        if len(km_thresholds) > 0:
            if m_probability_or_probabilities_km_distance is None:
                m_probability_or_probabilities_km_distance = [None] * len(km_thresholds)
            m_probability_or_probabilities_km_distance = ensure_is_iterable(
                m_probability_or_probabilities_km_distance
            )

            for thres, m_prob in zip(
                km_thresholds,
                m_probability_or_probabilities_km_distance,
            ):
                comparison_level = self._distance_in_km_level(
                    lat_col,
                    long_col,
                    km_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(comparison_level)

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        # Construct Description
        comparison_desc = ""
        if include_full_match_level:
            comparison_desc += "Exact match on full postcode vs. "

        if include_sector_match_level:
            comparison_desc += "exact match on sector vs. "

        if include_district_match_level:
            comparison_desc += "exact match on district vs. "

        if include_area_match_level:
            comparison_desc += "exact match on area vs. "

        if len(km_thresholds) > 0:
            desc = distance_threshold_description(
                col_name, "km_distance", km_thresholds
            )
            comparison_desc += desc

        comparison_desc += "all other comparisons"

        comparison_dict = {
            "output_column_name": col_name,
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)


class EmailComparisonBase(Comparison):
    def __init__(
        self,
        col_name: str,
        invalid_emails_as_null: bool = False,
        valid_email_regex: str = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+[.][a-zA-Z]{2,}$",
        term_frequency_adjustments_full: bool = False,
        include_exact_match_level: bool = True,
        include_username_match_level: bool = True,
        include_username_fuzzy_level: bool = True,
        include_domain_match_level: bool = False,
        levenshtein_thresholds: int | list = [],
        damerau_levenshtein_thresholds: int | list = [],
        jaro_winkler_thresholds: float | list = [0.88],
        jaro_thresholds: float | list = [],
        m_probability_full_match: bool = None,
        m_probability_username_match: bool = None,
        m_probability_or_probabilities_username_lev: float | list = None,
        m_probability_or_probabilities_username_dl: float | list = None,
        m_probability_or_probabilities_username_jw: float | list = None,
        m_probability_or_probabilities_username_jar: float | list = None,
        m_probability_or_probabilities_email_lev: float | list = None,
        m_probability_or_probabilities_email_dl: float | list = None,
        m_probability_or_probabilities_email_jw: float | list = None,
        m_probability_or_probabilities_email_jar: float | list = None,
        m_probability_domain_match: float | list = None,
        m_probability_else: float | list = None,
    ) -> Comparison:
        """A wrapped to generate a comparison for an email colummn
        'col_name' with preselected defaults.

        The default arguments will give a comparison with levels:\n
        - Exact match on email\n
        - Exact match on username with different domain\n
        - Fuzzy match on email user Jaro-Winkler\n
        - Fuzzy match on username using Jaro-Winkler \n
        - All other comparisons

        Args:
            col_name (str): The name of the column to compare.
            invalid_email_as_null (bool): If True, emails that do not adhere
                to valid_email_regex will be included in the null level.
                Defaults to False
            valid_email_regex (str): regular expression pattern that is used
                to validate emails. If invalid_emails_as_null is True,
                emails that do not adhere to valid_email_regex will be included
                 in the null level.
                 Defaults to "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
            term_frequency_adjustments_full (bool, optional): If True, apply
                term frequency adjustments to the full email exact match level.
                Defaults to False.
            include_exact_match_level (bool, optional): If True, include an exact
                match on full email level. Defaults to True.
            include_username_match_level (bool, optional): If True, include an exact
                match on username only level. Defaults to True.
            include_username_fuzzy_level (bool, optional): If True, include a level
                for fuzzy match on username. Defaults to True.
            include_domain_match_level (bool, optional): If True, include an exact
                match on domain only level. Defaults to True.
            levenshtein_thresholds (Union[int, list], optional): The thresholds
                to use for levenshtein similarity level(s).
                Defaults to []
            damerau_levenshtein_thresholds (Union[int, list], optional): The thresholds
                to use for damerau-levenshtein similarity level(s).
                Defaults to []
            jaro_winkler_thresholds (Union[int, list], optional): The thresholds
                to use for jaro_winkler similarity level(s).
                Defaults to [0.88]
            jaro_thresholds (Union[int, list], optional): The thresholds
                to use for jaro similarity level(s).
                Defaults to []
            m_probability_full_match (float, optional): Starting m
                probability for full match level. Defaults to None.
            m_probability_username_match (float, optional): Starting m probability
                for username only match level. Defaults to None.
            m_probability_or_probabilities_username_lev (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_username_dl (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_username_jw (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_username_jar (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_email_lev (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_email_dl (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_email_jw (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_email_jar (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_domain_match (float, optional): Starting m probability
                for domain only match level. Defaults to None.
            m_probability_else (float, optional): Starting m probability for
                the 'everything else' level. Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                Basic email Comparison
                ``` python
                import splink.duckdb.duckdb_comparison_template_library as ctl
                ctl.email_comparison("email")
                ```
                Bespoke email Comparison
                ``` python
                import splink.duckdb.duckdb_comparison_template_library as ctl
                ctl.email_comparison("email",
                                    levenshtein_thresholds = [2],
                                    damerau_levenshtein_thresholds = [2],
                                    invalid_emails_as_null = True,
                                    include_username_match_level = True,
                                    include_domain_match_level = True,
                                    )
                ```
            === ":simple-apachespark: Spark"
                Basic email Comparison
                ``` python
                import splink.spark.spark_comparison_template_library as ctl
                ctl.email_comparison(col_name = "email")
                ```
                Bespoke email Comparison
                ``` python
                import splink.spark.spark_comparison_template_library as ctl
                ctl.email_comparison("email",
                                    levenshtein_thresholds = [2],
                                    damerau_levenshtein_thresholds = [2],
                                    invalid_emails_as_null = True,
                                    include_username_match_level = True,
                                    include_domain_match_level = True,
                                    )

                ```

        Returns:
            Comparison: A comparison that can be inclued in the Splink settings
                dictionary.
        """
        # Contstruct comparrison

        comparison_levels = []

        # Decide whether invalid emails should be treated as null
        if invalid_emails_as_null:
            comparison_levels.append(
                self._null_level(col_name, valid_string_pattern=valid_email_regex)
            )
        else:
            comparison_levels.append(self._null_level(col_name))

        # Exact match on full email

        if include_exact_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                regex_extract=None,
                term_frequency_adjustments=term_frequency_adjustments_full,
                m_probability=m_probability_full_match,
                include_colname_in_charts_label=True,
            )
            comparison_levels.append(comparison_level)

        # Exact match on username with different domain

        if include_username_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                regex_extract="^[^@]+",
                m_probability=m_probability_username_match,
                include_colname_in_charts_label=True,
                manual_col_name_for_charts_label="Username",
            )
            comparison_levels.append(comparison_level)

        # Ensure fuzzy match thresholds are iterable

        damerau_levenshtein_thresholds = ensure_is_iterable(
            damerau_levenshtein_thresholds
        )
        levenshtein_thresholds = ensure_is_iterable(levenshtein_thresholds)
        jaro_winkler_thresholds = ensure_is_iterable(jaro_winkler_thresholds)
        jaro_thresholds = ensure_is_iterable(jaro_thresholds)

        # Fuzzy match on full email

        if len(levenshtein_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="levenshtein",
                distance_threshold_or_thresholds=levenshtein_thresholds,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_email_lev,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(damerau_levenshtein_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="damerau-levenshtein",
                distance_threshold_or_thresholds=damerau_levenshtein_thresholds,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_email_dl,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(jaro_winkler_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="jaro-winkler",
                distance_threshold_or_thresholds=jaro_winkler_thresholds,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_email_jw,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        if len(jaro_thresholds) > 0:
            threshold_levels = distance_threshold_comparison_levels(
                self,
                col_name,
                distance_function_name="jaro",
                distance_threshold_or_thresholds=jaro_thresholds,
                m_probability_or_probabilities_thres=m_probability_or_probabilities_email_jar,
                include_colname_in_charts_label=True,
            )
            comparison_levels = comparison_levels + threshold_levels

        # Fuzzy match on username only
        if include_username_fuzzy_level:
            if len(levenshtein_thresholds) > 0:
                threshold_levels = distance_threshold_comparison_levels(
                    self,
                    col_name,
                    regex_extract="^[^@]+",
                    distance_function_name="levenshtein",
                    distance_threshold_or_thresholds=levenshtein_thresholds,
                    m_probability_or_probabilities_thres=m_probability_or_probabilities_username_lev,
                    include_colname_in_charts_label=True,
                    manual_col_name_for_charts_label="Username",
                )
                comparison_levels = comparison_levels + threshold_levels

            if len(damerau_levenshtein_thresholds) > 0:
                threshold_levels = distance_threshold_comparison_levels(
                    self,
                    col_name,
                    regex_extract="^[^@]+",
                    distance_function_name="damerau-levenshtein",
                    distance_threshold_or_thresholds=damerau_levenshtein_thresholds,
                    m_probability_or_probabilities_thres=m_probability_or_probabilities_username_dl,
                    include_colname_in_charts_label=True,
                    manual_col_name_for_charts_label="Username",
                )
                comparison_levels = comparison_levels + threshold_levels

            if len(jaro_winkler_thresholds) > 0:
                threshold_levels = distance_threshold_comparison_levels(
                    self,
                    col_name,
                    regex_extract="^[^@]+",
                    distance_function_name="jaro-winkler",
                    distance_threshold_or_thresholds=jaro_winkler_thresholds,
                    m_probability_or_probabilities_thres=m_probability_or_probabilities_username_jw,
                    include_colname_in_charts_label=True,
                    manual_col_name_for_charts_label="Username",
                )
                comparison_levels = comparison_levels + threshold_levels

            if len(jaro_thresholds) > 0:
                threshold_levels = distance_threshold_comparison_levels(
                    self,
                    col_name,
                    distance_function_name="jaro",
                    distance_threshold_or_thresholds=jaro_thresholds,
                    m_probability_or_probabilities_thres=m_probability_or_probabilities_email_jar,
                    include_colname_in_charts_label=True,
                )
                comparison_levels = comparison_levels + threshold_levels

        # Domain-only match

        if include_domain_match_level:
            comparison_level = self._exact_match_level(
                col_name,
                regex_extract="@([^@]+)$",
                m_probability=m_probability_domain_match,
                manual_col_name_for_charts_label="Email Domain",
            )
            comparison_levels.append(comparison_level)

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        # Construct Description

        comparison_desc = ""
        if include_exact_match_level:
            comparison_desc += "Exact match vs. "

        if include_username_match_level:
            comparison_desc += "Exact username match different domain vs. "

        if len(levenshtein_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                "fuzzy email", "levenshtein", jaro_winkler_thresholds
            )
            comparison_desc += distance_threshold_description(
                "fuzzy username", "levenshtein", jaro_winkler_thresholds
            )

        if len(damerau_levenshtein_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                "fuzzy email", "damerau_levenshtein", jaro_winkler_thresholds
            )
            comparison_desc += distance_threshold_description(
                "fuzzy username", "levenshtein", jaro_winkler_thresholds
            )

        if len(jaro_winkler_thresholds) > 0:
            comparison_desc += distance_threshold_description(
                "fuzzy email", "jaro_winkler", jaro_winkler_thresholds
            )
            comparison_desc += distance_threshold_description(
                "fuzzy username", "jaro_winkler", jaro_winkler_thresholds
            )

        if include_domain_match_level:
            comparison_desc += "Domain-only match vs."

        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)

    @property
    def _is_distance_subclass(self):
        return False
