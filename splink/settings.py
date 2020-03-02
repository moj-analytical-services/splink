from collections import OrderedDict
import warnings


try:
    from pyspark.sql.session import SparkSession
except ImportError:
    SparkSession = None


from .case_statements import (
    _add_null_treatment_to_case_statement,
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
    default_case_statements = _get_default_case_statements_functions(spark)

    # Complete non-column settings
    non_col_keys = [
        "blocking_rules",
        "em_convergence",
        "unique_id_column_name",
        "additional_columns_to_retain",
        "retain_matching_columns",
        "retain_intermediate_calculation_columns",
        "max_iterations"
    ]
    for key in non_col_keys:
        if key not in settings_dict:
            settings_dict[key] = _get_default_value(key, is_column_setting=False)

    if len(settings_dict["blocking_rules"])==0:
        warnings.warn(
            """
            You have not specified any blocking rules, meaning all comparisons between the 
            input dataset(s) will be generated and blocking will not be used. 
            For large input datasets, this will generally be computationally intractable 
            because it will generate comparisons equal to the number of rows squared.
            """)        
            
    if "proportion_of_matches" not in settings_dict:
        settings_dict["proportion_of_matches"] = _get_default_value(
            "proportion_of_matches", is_column_setting=False
        )

    gamma_counter = 0
    for gamma_counter, column_settings in enumerate(
        settings_dict["comparison_columns"]
    ):

        column_settings["gamma_index"] = gamma_counter
        col_name = column_settings["col_name"]

        # Populate non-existing keys from defaults
        for key in [
            "num_levels",
            "data_type",
            "term_frequency_adjustments",
            "col_is_in_input_df",
        ]:
            if key not in column_settings:
                default = _get_default_value(key, is_column_setting=True)
                column_settings[key] = default

        levels = column_settings["num_levels"]

        if "case_expression" not in column_settings:
            data_type = column_settings["data_type"]
            col_name = column_settings["col_name"]
            case_fn = _get_default_case_statement_fn(
                default_case_statements, data_type, levels
            )
            column_settings["case_expression"] = case_fn(col_name, col_name)
        else:
            _check_no_obvious_problem_with_case_statement(
                column_settings["case_expression"]
            )
            old_case_stmt = column_settings["case_expression"]
            new_case_stmt = _add_null_treatment_to_case_statement(old_case_stmt)
            new_case_stmt = _add_as_gamma_to_case_statement(new_case_stmt, col_name)
            column_settings["case_expression"] = new_case_stmt

        if "m_probabilities" not in column_settings:
            levels = column_settings["num_levels"]
            probs = _get_probabilities("m", levels)
            column_settings["m_probabilities"] = probs
        else:
            levels = column_settings["num_levels"]
            probs = column_settings["m_probabilities"]

            if len(probs) != levels:
                raise ValueError(
                    "Number of m probabilities provided is not equal to number of levels specified"
                )

        if "u_probabilities" not in column_settings:
            levels = column_settings["num_levels"]
            probs = _get_probabilities("u", levels)
            column_settings["u_probabilities"] = probs
        else:
            levels = column_settings["num_levels"]
            probs = column_settings["u_probabilities"]
            if len(probs) != levels:
                raise ValueError(
                    "Number of m probabilities provided is not equal to number of levels specified"
                )

        column_settings["m_probabilities"] = _normalise_prob_list(
            column_settings["m_probabilities"]
        )
        column_settings["u_probabilities"] = _normalise_prob_list(
            column_settings["u_probabilities"]
        )

        gamma_counter += 1

    return settings_dict

