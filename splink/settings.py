from collections import OrderedDict
import warnings


try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None


from .case_statements import (
    _check_jaro_registered,
    _check_no_obvious_problem_with_case_statement,
    _add_as_gamma_to_case_statement,
    sql_gen_case_smnt_strict_equality_2,
    sql_gen_case_stmt_levenshtein_3,
    sql_gen_case_stmt_levenshtein_4,
    sql_gen_gammas_case_stmt_jaro_2,
    sql_gen_gammas_case_stmt_jaro_3,
    sql_gen_gammas_case_stmt_jaro_4,
    sql_gen_case_stmt_numeric_2,
    sql_gen_case_stmt_numeric_perc_3,
    sql_gen_case_stmt_numeric_perc_4,
)


from .validate import validate_settings, _get_default_value


def _normalise_prob_list(prob_array: list):
    sum_list = sum(prob_array)
    return [i / sum_list for i in prob_array]


def _get_default_case_statements_functions(spark):
    default_case_statements = {
        "numeric": {
            2: sql_gen_case_stmt_numeric_2,
            3: sql_gen_case_stmt_numeric_perc_3,
            4: sql_gen_case_stmt_numeric_perc_3,
        },
        "string": {},
    }

    jaro_exists = _check_jaro_registered(spark)

    if jaro_exists:
        default_case_statements["string"][2] = sql_gen_gammas_case_stmt_jaro_2
        default_case_statements["string"][3] = sql_gen_gammas_case_stmt_jaro_3
        default_case_statements["string"][4] = sql_gen_gammas_case_stmt_jaro_4

    else:
        default_case_statements["string"][2] = sql_gen_case_smnt_strict_equality_2
        default_case_statements["string"][3] = sql_gen_case_stmt_levenshtein_3
        default_case_statements["string"][4] = sql_gen_case_stmt_levenshtein_4

    return default_case_statements


def _get_columns_to_retain_df_e(settings):

    # Use ordered dict as an ordered set - i.e. to make sure we don't have duplicate cols to retain

    columns_to_retain = OrderedDict()
    columns_to_retain[settings["unique_id_column_name"]] = None

    if settings["retain_matching_columns"]:
        for c in settings["comparison_columns"]:
            if c["col_is_in_input_df"]:
                columns_to_retain[c["col_name"]] = None

    for c in settings["comparison_columns"]:
        if c["term_frequency_adjustments"]:
            columns_to_retain[c["col_name"]]

    for c in settings["additional_columns_to_retain"]:
        columns_to_retain[c] = None

    return columns_to_retain.keys()


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


def _get_probabilities(m_or_u, levels):

    if levels > 4:
        raise ValueError(
            f"No default m and u probabilities available when levels > 4, "
            "please specify custom values for 'm_probabilities' and 'u_probabilities' "
            "within your settings dictionary"
        )

    # Note all m and u probabilities are automatically normalised to sum to 1
    default_m_u_probabilities = {
        "m": {2: [1, 9], 3: [1, 2, 7], 4: [1, 1, 1, 7]},
        "u": {2: [9, 1], 3: [7, 2, 1], 4: [7, 1, 1, 1]},
    }

    probabilities = default_m_u_probabilities[m_or_u][levels]
    return _normalise_prob_list(probabilities)


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
        col_settings["case_expression"] = case_fn(col_name_for_case_fn, col_name_for_case_fn)
    else:
        _check_no_obvious_problem_with_case_statement(
            col_settings["case_expression"]
        )
        old_case_stmt = col_settings["case_expression"]
        new_case_stmt = _add_as_gamma_to_case_statement(old_case_stmt, col_name_for_case_fn)
        col_settings["case_expression"] = new_case_stmt


def _complete_probabilities(col_settings: dict, setting_name: str):
    """

    Args:
        col_settings (dict): Column settings dictionary
        setting_name (str): Either 'm_probabilities' or 'u_probabilities'

    """
    if setting_name == 'm_probabilities':
        letter = "m"
    elif setting_name == 'u_probabilities':
        letter = "u"

    if setting_name not in col_settings:
        levels = col_settings["num_levels"]
        probs = _get_probabilities(letter, levels)
        col_settings[setting_name] = probs
    else:
        levels = col_settings["num_levels"]
        probs = col_settings[setting_name]

        if len(probs) != levels:
            raise ValueError(
                f"Number of {setting_name} provided is not equal to number of levels specified"
            )

    col_settings[setting_name] = _normalise_prob_list(col_settings[setting_name])


def complete_settings_dict(settings_dict: dict, spark: SparkSession):
    """Auto-populate any missing settings from the settings dictionary using the 'sensible defaults' that
    are specified in the json schema (./splink/files/settings_jsonschema.json)

    Args:
        settings_dict (dict): The settings dictionary
        spark: The SparkSession

    Returns:
        dict: A `splink` settings dictionary with all keys populated.
    """
    validate_settings(settings_dict)

    # Complete non-column settings from their default values if not exist
    non_col_keys = [
        "em_convergence",
        "unique_id_column_name",
        "additional_columns_to_retain",
        "retain_matching_columns",
        "retain_intermediate_calculation_columns",
        "max_iterations",
        "proportion_of_matches"
    ]
    for key in non_col_keys:
        if key not in settings_dict:
            settings_dict[key] = _get_default_value(key, is_column_setting=False)

    if "blocking_rules" in settings_dict:
        if len(settings_dict["blocking_rules"])==0:
            warnings.warn(
                "You have not specified any blocking rules, meaning all comparisons between the "
                "input dataset(s) will be generated and blocking will not be used."
                "For large input datasets, this will generally be computationally intractable "
                "because it will generate comparisons equal to the number of rows squared.")

    gamma_counter = 0
    c_cols = settings_dict["comparison_columns"]
    for gamma_counter, col_settings in enumerate(c_cols):

        col_settings["gamma_index"] = gamma_counter

        # Populate non-existing keys from defaults
        keys_for_defaults = [
            "num_levels",
            "data_type",
            "term_frequency_adjustments",
        ]

        for key in keys_for_defaults:
            if key not in col_settings:
                default = _get_default_value(key, is_column_setting=True)
                col_settings[key] = default

        # Doesn't need assignment because we're modify the col_settings dictionary
        _complete_case_expression(col_settings, spark)
        _complete_probabilities(col_settings, "m_probabilities")
        _complete_probabilities(col_settings, "u_probabilities")

        gamma_counter += 1

    return settings_dict

