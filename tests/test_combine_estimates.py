import logging
from statistics import median, harmonic_mean


import pytest

from splink.combine_models import ModelCombiner, combine_cc_estimates
from splink.model import Model, load_model_from_json
from splink.settings import Settings

logger = logging.getLevelName


def test_api():
    model_1 = load_model_from_json("tests/params/params_1.json")
    model_2 = load_model_from_json("tests/params/params_2.json")
    model_3 = load_model_from_json("tests/params/params_3.json")

    first_name_cc = combine_cc_estimates(
        [
            model_2.current_settings_obj.get_comparison_column("first_name"),
            model_3.current_settings_obj.get_comparison_column("first_name"),
        ]
    )
    surname_cc = combine_cc_estimates(
        [
            model_1.current_settings_obj.get_comparison_column("surname"),
            model_3.current_settings_obj.get_comparison_column("surname"),
        ]
    )
    dob_cc = combine_cc_estimates(
        [
            model_1.current_settings_obj.get_comparison_column("dob"),
            model_2.current_settings_obj.get_comparison_column("dob"),
        ]
    )

    dict1 = {
        "name": "first name block",
        "model": model_1,
        "comparison_columns_for_global_lambda": [first_name_cc],
    }

    dict2 = {
        "name": "surname block",
        "model": model_2,
        "comparison_columns_for_global_lambda": [surname_cc],
    }

    dict3 = {
        "name": "dob block",
        "model": model_3,
        "comparison_columns_for_global_lambda": [dob_cc],
    }

    mc = ModelCombiner([dict1, dict2, dict3])

    mc.get_combined_settings_dict()
    mc.summary_report(aggregate_function=lambda x: sum(x) / len(x))
    mc.comparison_chart()


def test_average_calc_m_u(spark):
    settings_1 = {
        "link_type": "link_and_dedupe",
        "blocking_rules": ["l.forename = r.forename"],
        "comparison_columns": [
            {
                "col_name": "surname",
                "num_levels": 3,
                "m_probabilities": [0.1, 0.4, 0.5],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
            {
                "col_name": "email",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.9, 0.1],
            },
        ],
    }

    settings_2 = {
        "link_type": "link_and_dedupe",
        "blocking_rules": ["l.surname = r.surname"],
        "comparison_columns": [
            {
                "col_name": "forename",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.9, 0.1],
            },
            {
                "col_name": "email",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.85, 0.15],
            },
        ],
    }

    settings_3 = {
        "link_type": "link_and_dedupe",
        "blocking_rules": ["l.dob = r.dob"],
        "comparison_columns": [
            {
                "col_name": "forename",
                "num_levels": 3,
                "m_probabilities": [0.1, 0.4, 0.5],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
            {
                "col_name": "surname",
                "num_levels": 3,
                "m_probabilities": [0.2, 0.4, 0.4],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
            {
                "col_name": "email",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.7, 0.3],
            },
        ],
    }

    model_1 = Model(settings_1, spark)
    model_2 = Model(settings_2, spark)
    model_3 = Model(settings_3, spark)

    dict1 = {
        "name": "first name block",
        "model": model_1,
        # "comparison_columns_for_global_lambda": [first_name_cc],
    }

    dict2 = {
        "name": "surname block",
        "model": model_2,
        # "comparison_columns_for_global_lambda": [surname_cc],
    }

    dict3 = {
        "name": "dob block",
        "model": model_3,
        # "comparison_columns_for_global_lambda": [dob_cc],
    }

    mc = ModelCombiner([dict1, dict2, dict3])

    settings_dict = mc.get_combined_settings_dict(median)

    settings = Settings(settings_dict)
    email = settings.get_comparison_column("email")
    actual = email["u_probabilities"][0]
    expected = median([0.9, 0.85, 0.7])
    assert actual == pytest.approx(expected)

    surname = settings.get_comparison_column("surname")
    actual = surname["m_probabilities"][2]
    expected = median([0.4, 0.5])
    assert actual == pytest.approx(expected)

    assert len(settings_dict["blocking_rules"]) == 3

    settings_4_with_nulls = {
        "link_type": "link_and_dedupe",
        "blocking_rules": ["l.email = r.email"],
        "comparison_columns": [
            {
                "col_name": "forename",
                "num_levels": 3,
                "m_probabilities": [None, 0.4, 0.5],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
            {
                "col_name": "surname",
                "num_levels": 3,
                "m_probabilities": [0.1, 0.4, 0.5],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
        ],
    }

    model_4 = Model(settings_4_with_nulls, spark)

    dict4 = {
        "name": "email block",
        "model": model_4,
        # "comparison_columns_for_global_lambda": [dob_cc],
    }

    mc = ModelCombiner([dict1, dict2, dict3, dict4])

    with pytest.warns(UserWarning):
        settings_dict = mc.get_combined_settings_dict(median)

    settings = Settings(settings_dict)
    forename = settings.get_comparison_column("forename")
    actual = forename["m_probabilities"][0]
    assert actual is None


def test_global_lambda_calc(spark):

    # Imagine 10% of matches match in the cartesian product
    # Further imagine amongst records which are matches, 90% of the time surname matches
    # and that amongst records which are non-matches, 1% of the time surname matches
    # So:
    # 9% of records are matches with surname match
    # 0.9% are non-matches with surname match
    # So the proportion of mathes with a surnae match is 9/9.9 = 90.909090%

    settings_1 = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.90909090909090,
        "blocking_rules": ["l.surname = r.surname"],
        "comparison_columns": [
            {
                "col_name": "first_name",
                "m_probabilities": [0.3, 0.7],
                "u_probabilities": [0.8, 0.2],
            }
        ],
    }

    settings_2 = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.3,
        "blocking_rules": ["l.first_name = r.first_name"],
        "comparison_columns": [
            {
                "col_name": "surname",
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.99, 0.01],
            }
        ],
    }

    model_1 = Model(settings_1, spark)
    model_2 = Model(settings_2, spark)

    surname_cc = model_2.current_settings_obj.get_comparison_column("surname")
    dict1 = {
        "name": "dob and first name block",
        "model": model_1,
        "comparison_columns_for_global_lambda": [surname_cc],
    }

    mc = ModelCombiner([dict1])
    settings = mc.get_combined_settings_dict()
    assert settings["proportion_of_matches"] == pytest.approx(0.10)


# What follows is a much more resource intensive, but more comprehensive test
# that combining global lambda works
# from splink_data_generation.generate_data_exact import generate_df_gammas_exact
# from splink_data_generation.estimate_splink import estimate
# from copy import deepcopy
# settings = {
#     "link_type": "dedupe_only",
#     "proportion_of_matches": 0.6666666,
#     "blocking_rules": [

#     ],
#     "comparison_columns": [
#         {
#             "col_name": "first_name",
#             "m_probabilities": [0.3, 0.7],
#             "u_probabilities": [0.8, 0.2],
#         },
#         {
#             "col_name": "surname",
#             "m_probabilities": [0.1, 0.9],
#             "u_probabilities": [0.9, 0.1],
#         },
#         {
#             "col_name": "dob",
#             "m_probabilities": [0.3, 0.7],
#             "u_probabilities": [0.7, 0.3],
#         },
#         {
#             "col_name": "city",
#             "m_probabilities": [0.3, 0.7],
#             "u_probabilities": [0.95, 0.05],
#         },
#         {
#             "col_name": "email",
#             "m_probabilities": [0.05, 0.95],
#             "u_probabilities": [0.9, 0.1],
#         }
#     ]
# }
# from splink_data_generation.generate_data_exact import generate_df_gammas_exact
# from splink_data_generation.match_prob import add_match_prob
# from splink_data_generation.log_likelihood import add_log_likelihood
# df = generate_df_gammas_exact(settings)
# df = add_match_prob(df, settings)

# df.head()


# s_fn = deepcopy(settings)
# s_fn["comparison_columns"] = [c for c in s_fn["comparison_columns"] if c["col_name"] != "first_name"]
# s_fn["blocking_rules"] = ['l.first_name = r.first_name']


# df_fn = df[df["gamma_first_name"] == 1].copy()
# s_fn["proportion_of_matches"] = df_fn["true_match_probability_l"].mean()
# df_e_fn, model_fn = estimate(df_fn, s_fn, spark, compute_ll=False)

# model_fn.save_params_to_json_file("model_fn.json", overwrite=True)


# s_sn = deepcopy(settings)
# s_sn["comparison_columns"] = [c for c in s_sn["comparison_columns"] if c["col_name"] != "surname"]
# s_sn["blocking_rules"] = ['l.surname = r.surname']
# s_sn

# df_sn = df[df["gamma_surname"] == 1].copy()
# s_sn["proportion_of_matches"] = df_sn["true_match_probability_l"].mean()
# df_e_sn, model_sn = estimate(df_sn, s_sn, spark, compute_ll=False)

# model_sn.save_params_to_json_file("model_sn.json", overwrite=True)

# s_dob = deepcopy(settings)
# s_dob["comparison_columns"] = [c for c in s_dob["comparison_columns"] if c["col_name"] not in ["dob", "first_name"]]
# s_dob["blocking_rules"] = ['l.dob = r.dob and l.first_name = r.first_name']
# s_dob

# filter1=df["gamma_dob"] == 1
# filter2=df["gamma_first_name"] == 1
# df_dob = df[filter1 & filter2 ].copy()
# s_dob["proportion_of_matches"] = df_dob["true_match_probability_l"].mean()
# df_e_dob, model_dob = estimate(df_dob, s_dob, spark, compute_ll=False)

# from splink.combine_models import ModelCombiner, combine_cc_estimates

# fn_cc_1 = model_sn.current_settings_obj.get_comparison_column("first_name")

# sn_cc_1 = model_fn.current_settings_obj.get_comparison_column("surname")
# sn_cc_2 = model_dob.current_settings_obj.get_comparison_column("surname")

# sn_cc = combine_cc_estimates([sn_cc_1, sn_cc_2])

# dob_cc_1 = model_sn.current_settings_obj.get_comparison_column("dob")


# dict1 = {
#     "name": "first name block",
#     "model": model_fn,
#     "comparison_columns_for_global_lambda": [fn_cc_1]
# }

# dict2 = {
#     "name": "surname block",
#     "model": model_sn,
#     "comparison_columns_for_global_lambda": [sn_cc]
# }

# dict3 = {
#     "name": "dob and first name block",
#     "model": model_dob,
#     "comparison_columns_for_global_lambda": [dob_cc_1, fn_cc]
# }

# mc = ModelCombiner([dict1, dict2, dict3])
# mc.get_combined_settings_dict()
