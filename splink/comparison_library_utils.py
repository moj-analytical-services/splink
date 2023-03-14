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

    # Extracted from the DateDiffAtThresholdsComparisonBase class as that was overly
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
