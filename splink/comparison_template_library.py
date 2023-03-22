# Script defining comparison templates that act as wrapper functions which produce
# comparisons based on the type of data in a column with default values to make
# it simpler to use splink out-of-the-box

from __future__ import annotations

import logging

from .comparison import Comparison  # change to self
from .comparison_library_utils import datediff_error_logger
from .misc import ensure_is_iterable

logger = logging.getLogger(__name__)


class DateComparisonBase(Comparison):
    def __init__(
        self,
        col_name,
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        separate_1st_january=False,
        levenshtein_thresholds=[1, 2],
        jaro_winkler_thresholds=[],
        datediff_thresholds: int | list = [1, 10],
        datediff_metrics: str | list = ["year", "year"],
        m_probability_exact_match=None,
        m_probability_1st_january=None,
        m_probability_or_probabilities_lev: float | list = None,
        m_probability_or_probabilities_jw: float | list = None,
        m_probability_or_probabilities_datediff: float | list = None,
        m_probability_else=None,
    ):
        """A wrapper to generate a comparison for a date column the data in
        `col_name` with preselected defaults.

        The default arguments will give a comparison with comparison levels:\n
        - Exact match (1st of January only)\n
        - Exact match (all other dates)\n
        - Levenshtein distance <= 2\n
        - Date difference <= 1 year\n
        - Date difference <= 10 years \n
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            separate_1st_january (bool, optional): If True, include a separate
                exact match comparison level when date is 1st January.
            levenshtein_thresholds (Union[int, list], optional): The thresholds to use
                for levenshtein similarity level(s).
                We recommend use of either levenshtein or jaro_winkler for fuzzy
                matching, but not both.
                Defaults to [2]
            jaro_winkler_thresholds (Union[int, list], optional): The thresholds to use
                for jaro_winkler similarity level(s).
                We recommend use of either levenshtein or jaro_winkler for fuzzy
                matching, but not both.
                Defaults to []
            datediff_thresholds (Union[int, list], optional): The thresholds to use
                for datediff similarity level(s).
                Defaults to [1, 1].
            datediff_metrics (Union[str, list], optional): The metrics to apply
                thresholds to for datediff similarity level(s).
                Defaults to ["month", "year"].
            m_probability_exact_match (_type_, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_lev (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the levenshtein thresholds specified. Defaults to None.
            m_probability_or_probabilities_jw (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the jaro winkler thresholds specified. Defaults to None.
            m_probability_or_probabilities_datediff (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the datediff thresholds specified. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Returns:
            Comparison: A comparison that can be inclued in the Splink settings
                dictionary.
        """
        # Construct Comparison
        comparison_levels = []
        comparison_levels.append(self._null_level(col_name))

        # Validate user inputs
        datediff_error_logger(thresholds=datediff_thresholds, metrics=datediff_metrics)

        if separate_1st_january:
            comparison_level = {
                "sql_condition": f"""{col_name}_l = {col_name}_r AND
                                    substr({col_name}_l, 6, 5) = '01-01'""",
                "label_for_charts": "Matching and 1st Jan",
            }
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

        if len(levenshtein_thresholds) > 0:
            levenshtein_thresholds = ensure_is_iterable(levenshtein_thresholds)

            if m_probability_or_probabilities_lev is None:
                m_probability_or_probabilities_lev = [None] * len(
                    levenshtein_thresholds
                )
            m_probability_or_probabilities_lev = ensure_is_iterable(
                m_probability_or_probabilities_lev
            )

            for thres, m_prob in zip(
                levenshtein_thresholds, m_probability_or_probabilities_lev
            ):
                comparison_level = self._levenshtein_level(
                    col_name,
                    distance_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(comparison_level)

        if len(jaro_winkler_thresholds) > 0:
            jaro_winkler_thresholds = ensure_is_iterable(jaro_winkler_thresholds)

            if m_probability_or_probabilities_jw is None:
                m_probability_or_probabilities_jw = [None] * len(
                    jaro_winkler_thresholds
                )
            m_probability_or_probabilities_jw = ensure_is_iterable(
                m_probability_or_probabilities_jw
            )

            for thres, m_prob in zip(
                jaro_winkler_thresholds, m_probability_or_probabilities_jw
            ):
                comparison_level = self._jaro_winkler_level(
                    col_name,
                    distance_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(comparison_level)

        if len(levenshtein_thresholds) > 0 and len(jaro_winkler_thresholds) > 0:
            logger.warning(
                "You have included a comparison level for both Levenshtein and "
                "Jaro-Winkler similarity. We recommend choosing one or the other."
            )

        if len(datediff_thresholds) > 0:
            datediff_thresholds = ensure_is_iterable(datediff_thresholds)
            datediff_metrics = ensure_is_iterable(datediff_metrics)

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
            lev_desc = ", ".join([str(d) for d in levenshtein_thresholds])
            plural = "" if len(levenshtein_thresholds) == 1 else "s"
            comparison_desc += (
                f"Dates within levenshtein threshold{plural} {lev_desc} vs. "
            )

        if len(jaro_winkler_thresholds) > 0:
            lev_desc = ", ".join([str(d) for d in jaro_winkler_thresholds])
            plural = "" if len(jaro_winkler_thresholds) == 1 else "s"
            comparison_desc += (
                f"Dates within jaro_winkler threshold{plural} {lev_desc} vs. "
            )

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


class NameComparisonBase(Comparison):
    def __init__(
        self,
        col_name,
        include_exact_match_level=True,
        phonetic_col_name=None,
        term_frequency_adjustments_name=False,
        term_frequency_adjustments_phonetic_name=False,
        levenshtein_thresholds=[],
        jaro_winkler_thresholds=[0.95, 0.88],
        jaccard_thresholds=[],
        m_probability_exact_match_name=None,
        m_probability_exact_match_phonetic_name=None,
        m_probability_or_probabilities_lev: float | list = None,
        m_probability_or_probabilities_jw: float | list = None,
        m_probability_or_probabilities_jac: float | list = None,
        m_probability_else=None,
    ):
        """A wrapper to generate a comparison for a name column the data in
        `col_name` with preselected defaults.

        The default arguments will give a comparison with comparison levels:\n
        - Exact match \n
        - Jaro Winkler similarity >= 0.95\n
        - Jaro Winkler similarity >= 0.88\n
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            include_exact_match_level (bool, optional): If True, include an exact match
                level for col_name. Defaults to True.
            phonetic_col_name (str): The name of the column with phonetic reduction
                (such as dmetaphone) of col_name. Including parameter will create
                an exact match level for  phonetic_col_name. The phonetic column must
                be present in the dataset to use this parameter.
                Defaults to None
            term_frequency_adjustments_name (bool, optional): If True, apply term
                frequency adjustments to the exact match level for "col_name".
                Defaults to False.
            term_frequency_adjustments_phonetic_name (bool, optional): If True, apply
                term frequency adjustments to the exact match level for
                "phonetic_col_name".
                Defaults to False.
            levenshtein_thresholds (Union[int, list], optional): The thresholds to use
                for levenshtein similarity level(s).
                Defaults to []
            jaro_winkler_thresholds (Union[int, list], optional): The thresholds to use
                for jaro_winkler similarity level(s).
                Defaults to [0.88]
            jaccard_thresholds (Union[int, list], optional): The thresholds to use
                for jaccard similarity level(s).
                Defaults to []
            m_probability_exact_match_name (_type_, optional): If provided, overrides
                the default m probability for the exact match level for col_name.
                Defaults to None.
            m_probability_exact_match_phonetic_name (_type_, optional): If provided,
                overrides the default m probability for the exact match level for
                phonetic_col_name. Defaults to None.
            m_probability_or_probabilities_lev (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_datediff (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

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
                term_frequency_adjustments=term_frequency_adjustments_name,
                m_probability=m_probability_exact_match_name,
                include_colname_in_charts_label=True,
            )
            comparison_levels.append(comparison_level)

            if phonetic_col_name is not None:
                comparison_level = self._exact_match_level(
                    phonetic_col_name,
                    term_frequency_adjustments=term_frequency_adjustments_phonetic_name,
                    m_probability=m_probability_exact_match_phonetic_name,
                    include_colname_in_charts_label=True,
                )
                comparison_levels.append(comparison_level)

        if len(levenshtein_thresholds) > 0:
            levenshtein_thresholds = ensure_is_iterable(levenshtein_thresholds)

            if m_probability_or_probabilities_lev is None:
                m_probability_or_probabilities_lev = [None] * len(
                    levenshtein_thresholds
                )
            m_probability_or_probabilities_lev = ensure_is_iterable(
                m_probability_or_probabilities_lev
            )

            for thres, m_prob in zip(
                levenshtein_thresholds, m_probability_or_probabilities_lev
            ):
                comparison_level = self._levenshtein_level(
                    col_name,
                    distance_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(comparison_level)

        if len(jaro_winkler_thresholds) > 0:
            jaro_winkler_thresholds = ensure_is_iterable(jaro_winkler_thresholds)

            if m_probability_or_probabilities_jw is None:
                m_probability_or_probabilities_jw = [None] * len(
                    jaro_winkler_thresholds
                )
            m_probability_or_probabilities_jw = ensure_is_iterable(
                m_probability_or_probabilities_jw
            )

            for thres, m_prob in zip(
                jaro_winkler_thresholds, m_probability_or_probabilities_jw
            ):
                comparison_level = self._jaro_winkler_level(
                    col_name,
                    distance_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(comparison_level)

        if len(jaccard_thresholds) > 0:
            jaccard_thresholds = ensure_is_iterable(jaccard_thresholds)

            if m_probability_or_probabilities_jac is None:
                m_probability_or_probabilities_jac = [None] * len(jaccard_thresholds)
            m_probability_or_probabilities_jac = ensure_is_iterable(
                m_probability_or_probabilities_jac
            )

            for thres, m_prob in zip(
                jaccard_thresholds, m_probability_or_probabilities_jac
            ):
                comparison_level = self._jaccard_level(
                    col_name,
                    distance_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(comparison_level)

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
            lev_desc = ", ".join([str(d) for d in levenshtein_thresholds])
            plural = "" if len(levenshtein_thresholds) == 1 else "s"
            comparison_desc += (
                f"Dates within levenshtein threshold{plural} {lev_desc} vs. "
            )

        if len(jaro_winkler_thresholds) > 0:
            lev_desc = ", ".join([str(d) for d in jaro_winkler_thresholds])
            plural = "" if len(jaro_winkler_thresholds) == 1 else "s"
            comparison_desc += (
                f"Names within jaro_winkler threshold{plural} {lev_desc} vs. "
            )

        if len(jaccard_thresholds) > 0:
            lev_desc = ", ".join([str(d) for d in jaccard_thresholds])
            plural = "" if len(jaccard_thresholds) == 1 else "s"
            comparison_desc += f"Names within jaccard threshold{plural} {lev_desc} vs. "

        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)
