from copy import deepcopy
from splink.misc import bayes_factor_to_prob, prob_to_bayes_factor
from splink.comparison_library import levenshtein_at_thresholds, exact_match


first_name_cc = levenshtein_at_thresholds("first_name", 2, term_frequency_adjustments=True)
dob_cc = exact_match("dob")
email_cc = exact_match("email")
city_cc = exact_match("city")

surname_cc = {
    "output_column_name": "surname",
    "comparison_levels": [
        {
            "sql_condition": "surname_l IS NULL OR surname_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "surname_l = surname_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

bf_for_first_name = 0.9 / 0.1
glo = bayes_factor_to_prob(prob_to_bayes_factor(0.3) / bf_for_first_name)


def get_settings_dict():
    settings = {
        "probability_two_random_records_match": glo,
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.surname = r.surname",
        ],
        "comparisons": [
            first_name_cc,
            surname_cc,
            dob_cc,
            email_cc,
            city_cc,
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
        "additional_columns_to_retain": ["group"],
        "em_convergence": 0.001,
        "max_iterations": 20,
    }

    return deepcopy(settings)
