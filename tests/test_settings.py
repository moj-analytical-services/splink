from splink.settings import Settings


def test_overwrite():
    settings_orig = {
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.8, 0.2],
            },
            {
                "custom_name": "surname",
                "custom_columns_used": ["surname"],
                "num_levels": 3,
                "m_probabilities": [0.1, 0.2, 0.7],
                "u_probabilities": [0.5, 0.25, 0.25],
            },
            {
                "col_name": "first_name",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.8, 0.2],
            },
        ],
    }

    settings_overwrite = {
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "m_probabilities": [0.0, 1.0],
                "u_probabilities": [1.0, 0.0],
            },
            {
                "custom_name": "surname",
                "custom_columns_used": ["surname"],
                "num_levels": 3,
                "m_probabilities": [0.2, 0.2, 0.6],
                "u_probabilities": [0.6, 0.2, 0.2],
            },
            {
                "col_name": "other",
                "num_levels": 2,
                "m_probabilities": [0.33, 0.33],
                "u_probabilities": [0.33, 0.33],
            },
        ],
    }

    settings_obj = Settings(settings_orig)
    settings_obj.overwrite_m_u_probs_from_other_settings_dict(
        settings_overwrite, overwrite_m=False
    )

    cc = settings_obj.get_comparison_column("mob")
    assert cc["m_probabilities"][1] == 0.9
    assert cc["u_probabilities"][0] == 1.0

    cc = settings_obj.get_comparison_column("surname")
    assert cc["m_probabilities"][1] == 0.2
    assert cc["u_probabilities"][0] == 0.6

    expected_names = set(["mob", "surname", "first_name"])
    actual_names = set(settings_obj.comparison_column_dict.keys())
    assert expected_names == actual_names


def test_remove():
    settings = {
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.8, 0.2],
            },
            {
                "custom_name": "surname",
                "custom_columns_used": ["surname"],
                "num_levels": 3,
                "m_probabilities": [0.1, 0.2, 0.7],
                "u_probabilities": [0.5, 0.25, 0.25],
            },
        ],
    }
    settings_obj = Settings(settings)
    settings_obj.remove_comparison_column("mob")
    assert len(settings_obj["comparison_columns"]) == 1
    assert settings_obj["comparison_columns"][0]["custom_name"] == "surname"
    settings_obj.remove_comparison_column("surname")
    assert len(settings_obj["comparison_columns"]) == 0
