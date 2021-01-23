import warnings

from pyspark.sql.session import SparkSession

from copy import deepcopy

from .validate import validate_settings, _get_default_value
from .case_statements import (
    _check_jaro_registered,
    sql_gen_case_smnt_strict_equality_2,
    sql_gen_case_stmt_levenshtein_rel_3,
    sql_gen_case_stmt_levenshtein_rel_4,
    sql_gen_case_stmt_jaro_3,
    sql_gen_case_stmt_jaro_4,
    sql_gen_case_stmt_numeric_float_equality_2,
    sql_gen_case_stmt_numeric_perc_3,
    sql_gen_case_stmt_numeric_perc_4,
    _check_no_obvious_problem_with_case_statement,
    _add_as_gamma_to_case_statement,
)


def _normalise_prob_list(prob_array: list):
    sum_list = sum(prob_array)
    return [i / sum_list for i in prob_array]


def _get_default_case_statements_functions(spark):
    default_case_stmts = {
        "numeric": {},
        "string": {},
    }

    default_case_stmts["numeric"][2]: sql_gen_case_stmt_numeric_float_equality_2
    default_case_stmts["numeric"][3]: sql_gen_case_stmt_numeric_perc_3
    default_case_stmts["numeric"][4]: sql_gen_case_stmt_numeric_perc_4

    jaro_exists = _check_jaro_registered(spark)

    if jaro_exists:
        default_case_stmts["string"][2] = sql_gen_case_smnt_strict_equality_2
        default_case_stmts["string"][3] = sql_gen_case_stmt_jaro_3
        default_case_stmts["string"][4] = sql_gen_case_stmt_jaro_4

    else:
        default_case_stmts["string"][2] = sql_gen_case_smnt_strict_equality_2
        default_case_stmts["string"][3] = sql_gen_case_stmt_levenshtein_rel_3
        default_case_stmts["string"][4] = sql_gen_case_stmt_levenshtein_rel_4

    return default_case_stmts


def _get_default_case_statement_fn(default_statements, data_type, levels):
    if data_type not in ["string", "numeric"]:
        raise ValueError(
            f"No default case statement available for data type {data_type}, "
            "please specify a custom case_expression"
        )
    if levels > 4:
        raise ValueError(
            f"No default case statement available when levels > 4, "
            "please specify a custom 'case_expression' within your settings dictionary"
        )
    return default_statements[data_type][levels]


def _get_default_probabilities(m_or_u, levels):

    if levels > 5:
        raise ValueError(
            f"No default m and u probabilities available when levels > 4, "
            "please specify custom values for 'm_probabilities' and 'u_probabilities' "
            "within your settings dictionary"
        )

    # Note all m and u probabilities are automatically normalised to sum to 1
    default_m_u_probabilities = {
        "m_probabilities": {
            2: [1, 9],
            3: [1, 2, 7],
            4: [1, 1, 1, 7],
            5: [0.33, 0.67, 1, 2, 6],
        },
        "u_probabilities": {
            2: [9, 1],
            3: [7, 2, 1],
            4: [7, 1, 1, 1],
            5: [6, 2, 1, 0.33, 0.67],
        },
    }

    probabilities = default_m_u_probabilities[m_or_u][levels]
    return probabilities


def _complete_case_expression(col_settings, spark):

    default_case_statements = _get_default_case_statements_functions(spark)
    levels = col_settings["num_levels"]

    if "custom_name" in col_settings:
        col_name_for_case_fn = col_settings["custom_name"]
    else:
        col_name_for_case_fn = col_settings["col_name"]

    if "case_expression" not in col_settings:
        data_type = col_settings["data_type"]
        case_fn = _get_default_case_statement_fn(
            default_case_statements, data_type, levels
        )
        col_settings["case_expression"] = case_fn(
            col_name_for_case_fn, col_name_for_case_fn
        )
    else:
        _check_no_obvious_problem_with_case_statement(col_settings["case_expression"])
        old_case_stmt = col_settings["case_expression"]
        new_case_stmt = _add_as_gamma_to_case_statement(
            old_case_stmt, col_name_for_case_fn
        )
        col_settings["case_expression"] = new_case_stmt


def _complete_probabilities(col_settings: dict, setting_name: str):
    """

    Args:
        col_settings (dict): Column settings dictionary
        setting_name (str): Either 'm_probabilities' or 'u_probabilities'

    """

    if setting_name not in col_settings:
        levels = col_settings["num_levels"]
        probs = _get_default_probabilities(setting_name, levels)
        col_settings[setting_name] = probs
    else:
        levels = col_settings["num_levels"]
        probs = col_settings[setting_name]

        # Check for m and u manually set to zero (https://github.com/moj-analytical-services/splink/issues/161)
        if not all(col_settings[setting_name]):
            if "custom_name" in col_settings:
                col_name = col_settings["custom_name"]
            else:
                col_name = col_settings["col_name"]

            if setting_name == "m_probabilities":
                letter = "m"
            elif setting_name == "u_probabilities":
                letter = "u"

            warnings.warn(
                f"Your {setting_name} for {col_name} include zeroes. "
                f"Where {letter}=0 for a given level, it remains fixed rather than being estimated "
                "along with other model parameters, and all comparisons at this level "
                f"are assigned a match score of {1. if letter=='u' else 0.}, regardless of other comparisons columns."
            )

        if len(probs) != levels:
            raise ValueError(
                f"Number of {setting_name} provided is not equal to number of levels specified"
            )

    col_settings[setting_name] = col_settings[setting_name]


def complete_settings_dict(settings_dict: dict, spark: SparkSession):
    """Auto-populate any missing settings from the settings dictionary using the 'sensible defaults' that
    are specified in the json schema (./splink/files/settings_jsonschema.json)

    Args:
        settings_dict (dict): The settings dictionary
        spark: The SparkSession

    Returns:
        dict: A `splink` settings dictionary with all keys populated.
    """
    settings_dict = deepcopy(settings_dict)
    validate_settings(settings_dict)

    # Complete non-column settings from their default values if not exist
    non_col_keys = [
        "link_type",
        "em_convergence",
        "source_dataset_column_name",
        "unique_id_column_name",
        "additional_columns_to_retain",
        "retain_matching_columns",
        "retain_intermediate_calculation_columns",
        "max_iterations",
        "proportion_of_matches",
    ]
    for key in non_col_keys:
        if key not in settings_dict:
            settings_dict[key] = _get_default_value(key, is_column_setting=False)

    if "blocking_rules" in settings_dict:
        if len(settings_dict["blocking_rules"]) == 0:
            warnings.warn(
                "You have not specified any blocking rules, meaning all comparisons between the "
                "input dataset(s) will be generated and blocking will not be used."
                "For large input datasets, this will generally be computationally intractable "
                "because it will generate comparisons equal to the number of rows squared."
            )

    c_cols = settings_dict["comparison_columns"]
    for gamma_index, col_settings in enumerate(c_cols):

        col_settings["gamma_index"] = gamma_index

        # Populate non-existing keys from defaults
        keys_for_defaults = [
            "num_levels",
            "data_type",
            "term_frequency_adjustments",
            "fix_u_probabilities",
            "fix_m_probabilities",
        ]

        for key in keys_for_defaults:
            if key not in col_settings:
                default = _get_default_value(key, is_column_setting=True)
                col_settings[key] = default

        # Doesn't need assignment because we're modify the col_settings dictionary
        _complete_case_expression(col_settings, spark)
        _complete_probabilities(col_settings, "m_probabilities")
        _complete_probabilities(col_settings, "u_probabilities")

        if None not in col_settings["m_probabilities"]:
            col_settings["m_probabilities"] = _normalise_prob_list(
                col_settings["m_probabilities"]
            )
        else:
            warnings.warn(
                "Your m probabilities contain a None value "
                "so could not be normalised to 1"
            )

        if None not in col_settings["u_probabilities"]:
            col_settings["u_probabilities"] = _normalise_prob_list(
                col_settings["u_probabilities"]
            )
        else:
            warnings.warn(
                "Your u probabilities contain a None value "
                "so could not be normalised to 1"
            )

    return settings_dict
