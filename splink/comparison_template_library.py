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
        include_colname_in_charts_label=False,
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
                Defaults to [2]
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
                for the thresholds specified. Defaults to None.
            m_probability_or_probabilities_datediff (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the thresholds specified. Defaults to None.
            m_probability_else (_type_, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Returns:
            Comparison: A comparison that can be inclued in the Splink settings
                dictionary.
        """

        comparison_levels = []
        comparison_levels.append(self._null_level(col_name))

        # Validate user inputs
        datediff_error_logger(thresholds=datediff_thresholds, metrics=datediff_metrics)

        if separate_1st_january:
            level_dict = {
                "sql_condition": f"""{col_name}_l = {col_name}_r AND
                                    substr({col_name}_l, 6, 5) = '01-01'""",
                "label_for_charts": "Matching and 1st Jan",
            }
            if m_probability_1st_january:
                level_dict["m_probability"] = m_probability_1st_january
            if term_frequency_adjustments:
                level_dict["tf_adjustment_column"] = col_name
            comparison_levels.append(level_dict)

        if include_exact_match_level:
            level_dict = self._exact_match_level(
                col_name,
                term_frequency_adjustments=term_frequency_adjustments,
                m_probability=m_probability_exact_match,
            )
            comparison_levels.append(level_dict)

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
                level_dict = self._levenshtein_level(
                    col_name,
                    distance_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(level_dict)

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
                level_dict = self._jaro_winkler_level(
                    col_name,
                    distance_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(level_dict)

        if len(levenshtein_thresholds) > 0 and len(jaro_winkler_thresholds) > 0:
            logger.warning(
                "You have included a comparison levels for both Levenshtein and"
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
                level_dict = self._datediff_level(
                    col_name,
                    date_threshold=thres,
                    date_metric=metric,
                    m_probability=m_prob,
                )
                comparison_levels.append(level_dict)

            comparison_levels.append(
                self._else_level(m_probability=m_probability_else),
            )

            comparison_desc = ""
            if include_exact_match_level:
                comparison_desc += "Exact match vs. "

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
