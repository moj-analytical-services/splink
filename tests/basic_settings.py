from copy import deepcopy

from splink.internals.misc import bayes_factor_to_prob, prob_to_bayes_factor

first_name_cc = {
    "output_column_name": "first_name",
    "comparison_levels": [
        {
            "sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "first_name_l = first_name_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.7,
            "u_probability": 0.1,
            "tf_adjustment_column": "first_name",
            "tf_adjustment_weight": 0.6,
        },
        {
            "sql_condition": "levenshtein(first_name_l, first_name_r) <= 2",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "levenshtein <= 2",
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.8,
        },
    ],
}

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

dob_cc = {
    "output_column_name": "dob",
    "comparison_levels": [
        {
            "sql_condition": "dob_l IS NULL OR dob_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "dob_l = dob_r",
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

email_cc = {
    "output_column_name": "email",
    "comparison_levels": [
        {
            "sql_condition": "email_l IS NULL OR email_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "email_l = email_r",
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

city_cc = {
    "output_column_name": "city",
    "comparison_levels": [
        {
            "sql_condition": "city_l IS NULL OR city_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "city_l = city_r",
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
        "additional_columns_to_retain": ["cluster"],
        "em_convergence": 0.001,
        "max_iterations": 20,
    }

    return deepcopy(settings)


def name_comparison(cll, sn: str) -> dict:
    """A comparison using first and surname levels composed together."""
    return {
        "output_column_name": "first_name_and_surname",
        "comparison_levels": [
            # Null level
            cll.Or(cll.NullLevel("first_name"), cll.NullLevel(sn)),
            # Exact match on fn and sn
            cll.Or(
                cll.ExactMatchLevel("first_name"),
                cll.ExactMatchLevel(sn),
            ).configure(
                m_probability=0.8,
                label_for_charts="Exact match on first name or surname",
            ),
            # (Levenshtein(fn) and jaro_winkler(fn)) or levenshtein(sur)
            cll.And(
                cll.Or(
                    cll.LevenshteinLevel("first_name", 2),
                    cll.JaroWinklerLevel("first_name", 0.8),
                ).configure(m_probability=0.8),
                cll.LevenshteinLevel(sn, 3),
            ),
            cll.ElseLevel().configure(m_probability=0.1),
        ],
    }
