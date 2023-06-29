from .misc import ensure_is_iterable


def comparison_at_thresholds_error_logger(comparison, thresholds):
    error_logger = []

    if len(thresholds) == 0:
        error_logger.append(
            "`thresholds` must have at least one element, so that Comparison "
            "has more than just an 'else' level"
        )

    if any(size <= 0 for size in thresholds):
        error_logger.append("All entries of `thresholds` must be postive")

    if len(error_logger) > 0:
        error_logger.insert(
            0,
            f"The following error(s) were identified while validating "
            f"your arguments for `{comparison}_at_thresholds`:",
        )

        raise ValueError("\n\n".join(error_logger))

    return


def datediff_error_logger(thresholds, metrics):
    # Extracted from the DatediffAtThresholdsBase class as that was overly
    # verbose and failing the lint.

    error_logger = []

    if len(metrics) == 0:
        error_logger.append(
            "`date_metrics` must have at least one element, so that Comparison "
            "has more than just an 'else' level"
        )

    if len(thresholds) != len(metrics):
        error_logger.append(
            "There is a difference in length between your supplied "
            "`date_thresholds` and `date_metrics`. Please ensure that both "
            "arguments are of the same length before continuing."
        )

    if any(metric not in ["day", "month", "year"] for metric in metrics):
        error_logger.append(
            "`date_metrics` only accepts `day`, `month` and `year` as "
            "valid arguments."
        )

    if len(error_logger) > 0:
        error_logger.insert(
            0,
            "The following error(s) were identified while validating "
            "your arguments for `datediff_at_thresholds`:",
        )

        raise ValueError("\n\n".join(error_logger))

    return


def distance_threshold_comparison_levels(
    self,
    col_name: str,
    distance_function_name: str,
    distance_threshold_or_thresholds,
    regex_extract: str = None,
    set_to_lowercase: bool = False,
    higher_is_more_similar: bool = True,
    include_colname_in_charts_label=False,
    manual_col_name_for_charts_label=None,
    m_probability_or_probabilities_thres: list = None,
):
    thresholds = ensure_is_iterable(distance_threshold_or_thresholds)
    threshold_comparison_levels = []

    if m_probability_or_probabilities_thres is None:
        m_probability_or_probabilities_thres = [None] * len(thresholds)
    m_probability_or_probabilities_thres = ensure_is_iterable(
        m_probability_or_probabilities_thres
    )

    for thres, m_prob in zip(thresholds, m_probability_or_probabilities_thres):
        if distance_function_name == "levenshtein":
            distance_function_name = self._levenshtein_name
            higher_is_more_similar = False
        elif distance_function_name == "damerau-levenshtein":
            distance_function_name = self._damerau_levenshtein_name
            higher_is_more_similar = False
        elif distance_function_name == "jaro":
            distance_function_name = self._jaro_name
            higher_is_more_similar = True
        elif distance_function_name == "jaro-winkler":
            distance_function_name = self._jaro_winkler_name
            higher_is_more_similar = True
        elif distance_function_name == "jaccard":
            distance_function_name = self._jaccard_name
            higher_is_more_similar = True

        # these function arguments hold for all cases.
        kwargs = dict(
            col_name=col_name,
            distance_threshold=thres,
            include_colname_in_charts_label=include_colname_in_charts_label,
            manual_col_name_for_charts_label=manual_col_name_for_charts_label,
            regex_extract=regex_extract,
            set_to_lowercase=set_to_lowercase,
            m_probability=m_prob,
        )
        # separate out the two that are only used
        # when we have a user-supplied function, rather than a predefined subclass
        # feels a bit hacky, but will do at least for time being
        if not self._is_distance_subclass:
            kwargs["distance_function_name"] = distance_function_name
            kwargs["higher_is_more_similar"] = higher_is_more_similar

        level = self._distance_level(**kwargs)
        threshold_comparison_levels.append(level)

    return threshold_comparison_levels


def distance_threshold_description(
    column_description: str,
    distance_function_name: str,
    distance_threshold_or_thresholds: list,
):
    desc = ", ".join([str(d) for d in distance_threshold_or_thresholds])
    plural = "" if len(distance_threshold_or_thresholds) == 1 else "s"
    comparison_desc = (
        f"{column_description.title()} within {distance_function_name} "
        f"threshold{plural} {desc} vs. "
    )

    return comparison_desc
