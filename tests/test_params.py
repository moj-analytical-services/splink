from sparklink.params import Params
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

    param_example.save_params_to_iteration_history()
    param_example.reset_param_values_to_none()
    assert param_example.params["π"]["gamma_fname"]["prob_dist_match"]["level_0"]["probability"] is None
    param_example.populate_params(0.2, pi_df_collected)

    new_params = param_example.params

    assert new_params["π"]["gamma_fname"]["prob_dist_match"]["level_0"]["probability"] == 0.2
    assert new_params["π"]["gamma_fname"]["prob_dist_non_match"]["level_0"]["probability"] == 0.8

