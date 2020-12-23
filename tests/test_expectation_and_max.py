import os
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

from pyspark.sql import Row

from splink.maximisation_step import run_maximisation_step
from splink.blocking import block_using_rules
from splink.gammas import add_gammas
from splink.expectation_step import run_expectation_step
from splink.params import Params

from splink import Splink


def test_expectation_and_maximisation(spark):
    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.4,
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
                "case_expression": """
                    case
                    when surname_l is null or surname_r is null then -1
                    when surname_l = surname_r then 2
                    when substr(surname_l,1, 3) =  substr(surname_r, 1, 3) then 1
                    else 0
                    end
                    as gamma_surname
                    """,
                "m_probabilities": [0.1, 0.2, 0.7],
                "u_probabilities": [0.5, 0.25, 0.25],
            },
        ],
        "blocking_rules": [
            "l.mob = r.mob",
            "l.surname = r.surname",
        ],
        "retain_intermediate_calculation_columns": True,
    }

    rows = [
        {"unique_id": 1, "mob": 10, "surname": "Linacre"},
        {"unique_id": 2, "mob": 10, "surname": "Linacre"},
        {"unique_id": 3, "mob": 10, "surname": "Linacer"},
        {"unique_id": 4, "mob": 7, "surname": "Smith"},
        {"unique_id": 5, "mob": 8, "surname": "Smith"},
        {"unique_id": 6, "mob": 8, "surname": "Smith"},
        {"unique_id": 7, "mob": 8, "surname": "Jones"},
    ]

    df_input = spark.createDataFrame(Row(**x) for x in rows)
    df_input.persist()
    params = Params(settings, spark)

    df_comparison = block_using_rules(params.params.settings_dict, spark, df=df_input)
    df_gammas = add_gammas(df_comparison, params.params.settings_dict, spark)
    df_gammas.persist()
    df_e = run_expectation_step(df_gammas, params, params.params.settings_dict, spark)
    df_e = df_e.sort("unique_id_l", "unique_id_r")

    df_e.persist()

    ################################################
    # Test probabilities correctly assigned
    ################################################

    df = df_e.toPandas()
    cols_to_keep = [
        "prob_gamma_mob_match",
        "prob_gamma_mob_non_match",
        "prob_gamma_surname_match",
        "prob_gamma_surname_non_match",
    ]
    pd_df_result = df[cols_to_keep][:4]

    df_correct = [
        {
            "prob_gamma_mob_match": 0.9,
            "prob_gamma_mob_non_match": 0.2,
            "prob_gamma_surname_match": 0.7,
            "prob_gamma_surname_non_match": 0.25,
        },
        {
            "prob_gamma_mob_match": 0.9,
            "prob_gamma_mob_non_match": 0.2,
            "prob_gamma_surname_match": 0.2,
            "prob_gamma_surname_non_match": 0.25,
        },
        {
            "prob_gamma_mob_match": 0.9,
            "prob_gamma_mob_non_match": 0.2,
            "prob_gamma_surname_match": 0.2,
            "prob_gamma_surname_non_match": 0.25,
        },
        {
            "prob_gamma_mob_match": 0.1,
            "prob_gamma_mob_non_match": 0.8,
            "prob_gamma_surname_match": 0.7,
            "prob_gamma_surname_non_match": 0.25,
        },
    ]

    pd_df_correct = pd.DataFrame(df_correct)

    assert_frame_equal(pd_df_correct, pd_df_result)

    ################################################
    # Test match probabilities correctly calculated
    ################################################

    result_list = list(df["match_probability"])
    # See https://github.com/moj-analytical-services/splink/blob/master/tests/expectation_maximisation_test_answers.xlsx
    # for derivation of these numbers
    correct_list = [
        0.893617021,
        0.705882353,
        0.705882353,
        0.189189189,
        0.189189189,
        0.893617021,
        0.375,
        0.375,
    ]
    assert result_list == pytest.approx(correct_list)

    ################################################
    # Test new probabilities correctly calculated
    ################################################

    run_maximisation_step(df_e, params, spark)

    new_lambda = params.params["proportion_of_matches"]

    # See https://github.com/moj-analytical-services/splink/blob/master/tests/expectation_maximisation_test_answers.xlsx
    # for derivation of these numbers
    assert new_lambda == pytest.approx(0.540922141)

    rows = [
        ["mob", 0, 0.087438272, 0.441543191],
        ["mob", 1, 0.912561728, 0.558456809],
        ["surname", 0, 0.173315146, 0.340356209],
        ["surname", 1, 0.326240275, 0.160167628],
        ["surname", 2, 0.500444578, 0.499476163],
    ]

    settings_obj = params.params

    for r in rows:
        cc = settings_obj.get_comparison_column(r[0])
        level_dict = cc.level_as_dict(r[1])
        assert level_dict["m_probability"] == pytest.approx(r[2])
        assert level_dict["u_probability"] == pytest.approx(r[3])

    ################################################
    # Test revised probabilities correctly used
    ################################################

    df_e = run_expectation_step(df_gammas, params, params.params.settings_dict, spark)
    df_e = df_e.sort("unique_id_l", "unique_id_r")
    result_list = list(df_e.toPandas()["match_probability"])

    correct_list = [
        0.658602114,
        0.796821727,
        0.796821727,
        0.189486495,
        0.189486495,
        0.658602114,
        0.495063367,
        0.495063367,
    ]
    assert result_list == pytest.approx(correct_list)

    run_maximisation_step(df_e, params, spark)
    new_lambda = params.params["proportion_of_matches"]
    assert new_lambda == pytest.approx(0.534993426)

    rows = [
        ["mob", 0, 0.088546179, 0.435753788],
        ["mob", 1, 0.911453821, 0.564246212],
        ["surname", 0, 0.231340865, 0.27146747],
        ["surname", 1, 0.372351177, 0.109234086],
        ["surname", 2, 0.396307958, 0.619298443],
    ]

    settings_obj = params.params

    for r in rows:
        cc = settings_obj.get_comparison_column(r[0])
        level_dict = cc.level_as_dict(r[1])
        assert level_dict["m_probability"] == pytest.approx(r[2])
        assert level_dict["u_probability"] == pytest.approx(r[3])

    ################################################
    # Test whether saving and loading params works
    # (If we load params, does the expectation step yield same answer)
    ################################################
    import tempfile

    dir = tempfile.TemporaryDirectory()
    fname = os.path.join(dir.name, "params.json")

    df_e = run_expectation_step(df_gammas, params, params.params.settings_dict, spark)
    params.save_params_to_json_file(fname)

    from splink.params import load_params_from_json

    p = load_params_from_json(fname)

    df_e_2 = run_expectation_step(df_gammas, p, p.params.settings_dict, spark)

    assert_frame_equal(df_e.toPandas(), df_e_2.toPandas())
