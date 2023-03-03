# Script defining comparison templates that act as wrapper functions which produce
# comparisons based on the type of data in a column with default values to make
# it simpler to use splink out-of-the-box

from __future__ import annotations

from .comparison import Comparison  # change to self

# import splink.duckdb.duckdb_comparison_level_library as cll  # change to self

# from .comparison_level_library

# from .comparison_library_utils import datediff_error_logger
from .misc import ensure_is_iterable


class DateComparisonBase(Comparison):
    def __init__(
        self,
        col_name,
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        separate_1st_january=True,
        levenshtein_thresholds=[2],
        datediff_thresholds=[[1, 1], ["month", "year"]],
        m_probability_exact_match=None,
        m_probability_1st_january=None,
        m_probability_or_probabilities_lev: float | list = None,
        m_probability_or_probabilities_datediff: float | list = None,
        m_probability_else=None,
        include_colname_in_charts_label=False,
    ):

        """A wrapper to generate a comparison for a date column the data in `col_name` with preselected defaults.

        The default arguments will give output:
        - Exact match (1st of January only)
        - Exact match (all other dates)
        - Levenshtein distance <= 2
        - Date difference <= 1 month
        - Date difference <= 1 year
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            separate_1st_january (bool, optional): If True, include a separate comparison
                level for 1st January.
            levenshtein_thresholds (Union[int, list], optional): The thresholds to use
                for levenshtein similarity level(s).
                Defaults to [2]
            datediff_thresholds (Union[int, str, list], optional): The thresholds to use
                for datediff similarity level(s).
                Defaults to [["month", "year"], [1, 1]].
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
        # add check for date format to start

        comparison_levels = []
        comparison_levels.append(self._null_level(col_name))

        if separate_1st_january:
            level_dict = {
                "sql_condition": f"{col_name}_l = {col_name}_r AND substr({col_name}_l, -5, 5) = '01-01'",
                "label_for_charts": "Matching and 1st Jan",
            }
            if m_probability_1st_january:
                level_dict["m_probability"] = m_probability_1st_january
            if term_frequency_adjustments:
                level_dict["tf_adjustment_column"] = col_name
            print(level_dict)
            comparison_levels.append(level_dict)

        if include_exact_match_level:
            level_dict = self._exact_match_level(  # change so self._exact_match_level
                col_name,
                term_frequency_adjustments=term_frequency_adjustments,
                m_probability=m_probability_exact_match,
            )

            # description = "Exact match vs. "
            print(level_dict)
            comparison_levels.append(level_dict)

        if len(levenshtein_thresholds) > 0:
            # print(f"levenshtein: {levenshtein_thresholds}")
            levenshtein_thresholds = ensure_is_iterable(levenshtein_thresholds)

            if m_probability_or_probabilities_lev is None:
                m_probability_or_probabilities_lev = [None] * len(
                    levenshtein_thresholds
                )
            m_probabilities = ensure_is_iterable(m_probability_or_probabilities_lev)

            for thres, m_prob in zip(
                levenshtein_thresholds, m_probability_or_probabilities_lev
            ):
                level_dict = self._levenshtein_level(
                    col_name,
                    distance_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(level_dict)

        if len(datediff_thresholds[0]) > 0:
            print(f"datediff: {datediff_thresholds}")
            # print(f"datediff: {datediff_thresholds}")
            datediff_thresholds = ensure_is_iterable(datediff_thresholds)

            if m_probability_or_probabilities_datediff is None:
                m_probability_or_probabilities_datediff = [None] * len(
                    datediff_thresholds
                )
            m_probabilities = ensure_is_iterable(
                m_probability_or_probabilities_datediff
            )

            for thres, metric, m_prob in zip(
                datediff_thresholds[0],
                datediff_thresholds[1],
                m_probability_or_probabilities_datediff,
            ):
                level_dict = self._datediff_level(
                    col_name,
                    date_metric=metric,
                    date_threshold=thres,
                    m_probability=m_prob,
                )
                comparison_levels.append(level_dict)

            comparison_levels.append(
                self._else_level(m_probability=m_probability_else),
            )

            comparison_desc = ""
            if include_exact_match_level:
                comparison_desc += "Exact match vs. "

            if len(levenshtein_thresholds) > 0:
                lev_desc = ", ".join([str(d) for d in levenshtein_thresholds])
                plural = "" if len(levenshtein_thresholds) == 1 else "s"
                comparison_desc += (
                    f"Dates within Levenshtein threshold{plural} {lev_desc} vs. "
                )

        if len(datediff_thresholds[0]) > 0:
            datediff_desc = ", ".join(
                [
                    f"{m.title()}(s): {v}"
                    for v, m in zip(datediff_thresholds[0], datediff_thresholds[1])
                ]
            )
            plural = "" if len(datediff_thresholds[0]) == 1 else "s"
            comparison_desc += (
                f"Dates within the following threshold{plural} {datediff_desc} vs. "
            )

        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)
