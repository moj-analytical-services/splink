from splink.params import Params, get_or_update_settings
import pytest

# Light testing at the moment.  Focus on aspects that could break main algo

@pytest.fixture(scope='module')
def param_example():
    gamma_settings = {
                    "link_type": "dedupe_only",
                      "proportion_of_matches": 0.2,
                     "comparison_columns": [
                        {"col_name": "fname"},
                        {"col_name": "sname",
                        "num_levels": 3}
                    ],
                    "blocking_rules": []
                    }

    params = Params(gamma_settings, spark="supress_warnings")

    yield params


# Test param updates
def test_prob_sum_one(param_example):

    p = param_example.params

    for m in ["prob_dist_match", "prob_dist_non_match"]:
        for g in ["gamma_fname", "gamma_sname"]:
            levels  = p["π"][g][m]

            total = 0
            for l in levels:
                total += levels[l]["probability"]

            assert total == pytest.approx(1.0)

def test_update(param_example):


    pi_df_collected = [
     {'gamma_value': 1, 'new_probability_match': 0.9, 'new_probability_non_match': 0.1, 'gamma_col': 'gamma_fname'},
     {'gamma_value': 0, 'new_probability_match': 0.2, 'new_probability_non_match': 0.8, 'gamma_col': 'gamma_fname'},
     {'gamma_value': 1, 'new_probability_match': 0.9, 'new_probability_non_match': 0.1, 'gamma_col': 'gamma_sname'},
     {'gamma_value': 2, 'new_probability_match': 0.7, 'new_probability_non_match': 0.3, 'gamma_col': 'gamma_sname'},
     {'gamma_value': 0, 'new_probability_match': 0.5, 'new_probability_non_match': 0.5, 'gamma_col': 'gamma_sname'}]

    param_example._save_params_to_iteration_history()
    param_example._reset_param_values_to_none()
    assert param_example.params["π"]["gamma_fname"]["prob_dist_match"]["level_0"]["probability"] is None
    param_example._populate_params(0.2, pi_df_collected)

    new_params = param_example.params

    assert new_params["π"]["gamma_fname"]["prob_dist_match"]["level_0"]["probability"] == 0.2
    assert new_params["π"]["gamma_fname"]["prob_dist_non_match"]["level_0"]["probability"] == 0.8

def test_update_settings():
    
    old_settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.2,
        "comparison_columns": [
            {"col_name": "fname"},
            {"col_name": "sname",
             "num_levels": 3}
        ],
        "blocking_rules": []
    }

    params = Params(old_settings, spark="supress_warnings")
    
    new_settings = {
        "link_type": "dedupe_only",
        "blocking_rules": [],
        "comparison_columns": [
            {
                "col_name": "fname",
                "num_levels": 3,
                "m_probabilities": [0.02,0.03,0.95],
                "u_probabilities": [0.92,0.05,0.03]
            },
            {
                "custom_name": "sname",
                "custom_columns_used": ["fname", "sname"],
                "num_levels": 3,
                "case_expression": """
                    case when concat(fname_l, sname_l) = concat(fname_r, sname_r) then 1
                    else 0 end
                """,
                "m_probabilities": [0.01,0.02,0.97],
                "u_probabilities": [0.9,0.05,0.05]
            },
            {"col_name": "dob"}
        ]
    }
    
    update = get_or_update_settings(params, new_settings)
    
    # new settings used due to num_levels mismatch
    assert update["comparison_columns"][0]["m_probabilities"] == new_settings["comparison_columns"][0]["m_probabilities"]
    # new settings updated with old settings 
    assert update["comparison_columns"][1]["u_probabilities"] == pytest.approx(params.settings["comparison_columns"][1]["u_probabilities"])
    