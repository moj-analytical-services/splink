import warnings

from pyspark.sql.session import SparkSession

from copy import deepcopy

from .validate import get_default_value_from_schema

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

    default_case_stmts["numeric"][2] = sql_gen_case_stmt_numeric_float_equality_2
    default_case_stmts["numeric"][3] = sql_gen_case_stmt_numeric_perc_3
    default_case_stmts["numeric"][4] = sql_gen_case_stmt_numeric_perc_4

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

    if levels > 6:
        raise ValueError(
            f"No default m and u probabilities available when levels > 6, "
            "please specify custom values for 'm_probabilities' and 'u_probabilities' "
            "within your settings dictionary"
        )

    # Note all m and u probabilities are automatically normalised to sum to 1
    default_m_u_probabilities = {
        "m_probabilities": {
            2: _normalise_prob_list([1, 9]),
            3: _normalise_prob_list([1, 2, 7]),
            4: _normalise_prob_list([1, 1, 1, 7]),
            5: _normalise_prob_list([0.33, 0.67, 1, 2, 6]),
            6: _normalise_prob_list([0.33, 0.67, 1, 2, 3, 6]),
        },
        "u_probabilities": {
            2: _normalise_prob_list([9, 1]),
            3: _normalise_prob_list([7, 2, 1]),
            4: _normalise_prob_list([7, 1, 1, 1]),
            5: _normalise_prob_list([6, 2, 1, 0.33, 0.67]),
            6: _normalise_prob_list([6, 3, 2, 1, 0.33, 0.67]),
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


def _complete_probabilities(col_settings: dict, mu_probabilities: str):
    """

    Args:
        col_settings (dict): Column settings dictionary
        mu_probabilities (str): Either 'm_probabilities' or 'u_probabilities'

    """

    if mu_probabilities not in col_settings:
        levels = col_settings["num_levels"]
        probs = _get_default_probabilities(mu_probabilities, levels)
        col_settings[mu_probabilities] = probs


def _complete_tf_adjustment_weights(col_settings: dict):

    if "tf_adjustment_weights" in col_settings:
        if not all(0.0 <= w <= 1.0 for w in col_settings["tf_adjustment_weights"]):
            raise ValueError(
                f"All values of 'tf_adjustment_weights' must be between 0 and 1"
            )
    else:
        weights = [0.0] * col_settings["num_levels"]
        weights[-1] = 1.0  
        col_settings["tf_adjustment_weights"] = weights


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
            settings_dict[key] = get_default_value_from_schema(
                key, is_column_setting=False
            )

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

        # Gamma index refers to the position in the comparison vector
        # i.e. it's a counter for comparison columns
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
                default = get_default_value_from_schema(key, is_column_setting=True)
                col_settings[key] = default

        # Doesn't need assignment because we're modify the col_settings dictionary
        _complete_case_expression(col_settings, spark)
        _complete_probabilities(col_settings, "m_probabilities")
        _complete_probabilities(col_settings, "u_probabilities")
        _complete_tf_adjustment_weights(col_settings)

    return settings_dict


def normalise_probabilities(settings_dict: dict):
    """Normalise all probabilities in a settings dictionary to sum
    to one, of possible

    Args:
        settings_dict (dict): Splink settings dictionary
    """

    c_cols = settings_dict["comparison_columns"]
    for col_settings in c_cols:
        for p in ["m_probabilities", "u_probabilities"]:
            if p in col_settings:
                if None not in col_settings[p]:
                    if sum(col_settings[p]) != 0:
                        col_settings[p] = _normalise_prob_list(col_settings[p])
    return settings_dict
