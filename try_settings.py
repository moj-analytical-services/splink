import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


first_name_cc = {
    "column_name": "first_name",
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
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "levenshtein(first_name_l, first_name_r) <= 2",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "Levenstein <= 2",
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
    "column_name": "surname",
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
    "column_name": "dob",
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
    "column_name": "email",
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
    "column_name": "city",
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
            "tf_adjustment_column": "city",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}


settings_dict = {
    "proportion_of_matches": 0.01,
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.dob = r.dob",
        "l.city = r.city and l.first_name = r.first_name",
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
    "max_iterations": 10,
}
