from splink.params import Params
import pytest

# Light testing at the moment.  Focus on aspects that could break main algo


@pytest.fixture(scope="module")
def param_example(spark):

    case_expr = """
    case
    when email_l is null or email_r is null then -1
    when email_l = email_r then 1
    else 0
    end
    as gamma_my_custom
    """

    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.2,
        "comparison_columns": [
            {"col_name": "fname"},
            {"col_name": "sname", "num_levels": 3},
            {
                "custom_name": "my_custom",
                "custom_columns_used": ["email", "city"],
                "case_expression": case_expr,
                "num_levels": 2,
            },
        ],
        "blocking_rules": [],
    }

    params = Params(settings, spark=spark)

    yield params


# Test param updates
def test_prob_sum_one(param_example):

    p = param_example.params

    for cc in p.comparison_columns:
        assert sum(cc["m_probabilities"]) == pytest.approx(1.0)
        assert sum(cc["u_probabilities"]) == pytest.approx(1.0)


def test_update(param_example):

    pi_df_collected = [
        {
            "gamma_value": 1,
            "new_probability_match": 0.9,
            "new_probability_non_match": 0.1,
            "column_name": "fname",
        },
        {
            "gamma_value": 0,
            "new_probability_match": 0.2,
            "new_probability_non_match": 0.8,
            "column_name": "fname",
        },
        {
            "gamma_value": 1,
            "new_probability_match": 0.9,
            "new_probability_non_match": 0.1,
            "column_name": "sname",
        },
        {
            "gamma_value": 2,
            "new_probability_match": 0.7,
            "new_probability_non_match": 0.3,
            "column_name": "sname",
        },
        {
            "gamma_value": 0,
            "new_probability_match": 0.5,
            "new_probability_non_match": 0.5,
            "column_name": "sname",
        },
        {
            "gamma_value": 0,
            "new_probability_match": 0.4,
            "new_probability_non_match": 0.6,
            "column_name": "my_custom",
        },
        {
            "gamma_value": 1,
            "new_probability_match": 0.9,
            "new_probability_non_match": 0.1,
            "column_name": "my_custom",
        },
    ]

    param_example.save_params_to_iteration_history()
    param_example.params.reset_all_probabilities()

    assert (
        param_example.params.get_comparison_column("fname")["m_probabilities"][0] is 0
    )
    param_example._populate_params_from_maximisation_step(0.2, pi_df_collected)

    new_params = param_example.params

    assert new_params.get_comparison_column("fname")["m_probabilities"][0] == 0.2
    assert new_params.get_comparison_column("fname")["u_probabilities"][0] == 0.8
