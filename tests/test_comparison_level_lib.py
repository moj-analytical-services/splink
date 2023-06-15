import pandas as pd

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_column_reversal(test_helpers, dialect):
    helper = test_helpers[dialect]
    cll = helper.cll

    data = [
        {"id": 1, "forename": "John", "surname": "Smith", "full_name": "John Smith"},
        {"id": 2, "forename": "Smith", "surname": "John", "full_name": "Smith John"},
        {"id": 3, "forename": "Rob", "surname": "Jones", "full_name": "Rob Jones"},
        {"id": 4, "forename": "Rob", "surname": "Jones", "full_name": "Rob Jones"},
    ]

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [
            {
                "output_column_name": "full_name",
                "comparison_levels": [
                    cll.null_level("full_name"),
                    cll.exact_match_level("full_name"),
                    cll.columns_reversed_level("forename", "surname"),
                    cll.else_level(),
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    df = pd.DataFrame(data)
    df = helper.convert_frame(df)

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.predict().as_pandas_dataframe()

    row = dict(df_e.query("id_l == 1 and id_r == 2").iloc[0])
    assert row["gamma_full_name"] == 1

    row = dict(df_e.query("id_l == 3 and id_r == 4").iloc[0])
    assert row["gamma_full_name"] == 2


@mark_with_dialects_excluding()
def test_perc_difference(test_helpers, dialect):
    helper = test_helpers[dialect]
    cll = helper.cll

    data = [
        {"id": 1, "amount": 1.2},
        {"id": 2, "amount": 1.0},
        {"id": 3, "amount": 0.8},
        {"id": 4, "amount": 200},
        {"id": 5, "amount": 140},
    ]

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [
            {
                "output_column_name": "amount",
                "comparison_levels": [
                    cll.null_level("amount"),
                    cll.percentage_difference_level("amount", 0.0),  # 4
                    cll.percentage_difference_level("amount", (0.2 / 1.2) + 1e-4),  # 3
                    cll.percentage_difference_level("amount", (0.2 / 1.0) + 1e-4),  # 2
                    cll.percentage_difference_level("amount", (60 / 200) + 1e-4),  # 1
                    cll.else_level(),
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    df = pd.DataFrame(data)
    df = helper.convert_frame(df)

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.predict().as_pandas_dataframe()

    row = dict(df_e.query("id_l == 1 and id_r == 2").iloc[0])  # 16.66%
    assert row["gamma_amount"] == 3

    row = dict(df_e.query("id_l == 2 and id_r == 3").iloc[0])  # 20%
    assert row["gamma_amount"] == 2

    row = dict(df_e.query("id_l == 4 and id_r == 5").iloc[0])  # 30%
    assert row["gamma_amount"] == 1
