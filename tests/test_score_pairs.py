import datetime

import pytest

import splink.internals.comparison_library as cl
from splink import SettingsCreator
from splink.internals.blocking_rule_library import block_on

from .decorator import mark_with_dialects_excluding


def _settings():
    return SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name").configure(term_frequency_adjustments=True),
            cl.ExactMatch("surname"),
            cl.DateOfBirthComparison("dob", input_is_string=False),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.ExactMatch("email"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("surname"),
        ],
        max_iterations=2,
        retain_intermediate_calculation_columns=True,
        retain_matching_columns=True,
    )


@mark_with_dialects_excluding("sqlite")
def test_score_pair_tf_table_and_derived(test_helpers, dialect, fake_1000):
    # - User provides a city tf table
    # - first_name tf table derived from input data
    helper = test_helpers[dialect]

    linker = helper.linker_with_registration(fake_1000, _settings())

    city_tf = [
        {"city": "London", "tf_city": 0.2},
        {"city": "Liverpool", "tf_city": 0.8},
    ]
    linker.table_management.register_term_frequency_lookup(city_tf, "city")

    r1 = {
        "first_name": "Julia ",
        "surname": "Taylor",
        "dob": datetime.date(2015, 10, 29),
        "city": "London",
        "email": "hannah88@powers.com",
    }
    r2 = {
        "first_name": "Julia ",
        "surname": "Taylor",
        "dob": datetime.date(2015, 10, 29),
        "city": "London",
        "email": "hannah88@powers.com",
    }

    res = linker.inference.score_pair(r1, r2)
    res_dict = res.as_dict()

    assert res_dict["tf_city_l"][0] == 0.2
    assert res_dict["tf_city_r"][0] == 0.2
    assert pytest.approx(res_dict["tf_first_name_l"][0]) == 0.00444444444444
    assert pytest.approx(res_dict["tf_first_name_r"][0]) == 0.00444444444444


@mark_with_dialects_excluding("sqlite")
def test_score_pair_input_values_take_precedence(test_helpers, dialect, fake_1000):
    # - User provides city and first_name tf tables
    # - But specific values provided in input data, which take precedence
    helper = test_helpers[dialect]

    linker = helper.linker_with_registration(fake_1000, _settings())

    city_tf = [
        {"city": "London", "tf_city": 0.2},
        {"city": "Liverpool", "tf_city": 0.8},
    ]
    linker.table_management.register_term_frequency_lookup(city_tf, "city")

    first_name_tf = [
        {"first_name": "Julia", "tf_first_name": 0.3},
        {"first_name": "Robert", "tf_first_name": 0.8},
    ]
    linker.table_management.register_term_frequency_lookup(first_name_tf, "first_name")

    r1 = {
        "first_name": "Julia",
        "surname": "Taylor",
        "dob": datetime.date(2015, 10, 29),
        "city": "London",
        "email": "hannah88@powers.com",
        "tf_city": 0.5,
    }
    r2 = {
        "first_name": "Julia",
        "surname": "Taylor",
        "dob": datetime.date(2015, 10, 29),
        "city": "London",
        "email": "hannah88@powers.com",
        "tf_first_name": 0.4,
    }

    res = linker.inference.score_pair(r1, r2)
    res_dict = res.as_dict()

    assert res_dict["tf_city_l"][0] == 0.5
    assert res_dict["tf_city_r"][0] == 0.2
    assert res_dict["tf_first_name_l"][0] == 0.3
    assert res_dict["tf_first_name_r"][0] == 0.4


def _records():
    r1 = {
        "first_name": "Julia ",
        "surname": "Taylor",
        "dob": datetime.date(2015, 10, 29),
        "city": "London",
        "email": "hannah88@powers.com",
        "tf_city": 0.5,
        "tf_first_name": 0.1,
    }
    r2 = {
        "first_name": "Robert",
        "surname": "Smith",
        "dob": datetime.date(1990, 1, 1),
        "city": "Liverpool",
        "email": "robert@smith.com",
        "tf_city": 0.5,
        "tf_first_name": 0.1,
    }
    return r1, r2


@mark_with_dialects_excluding("sqlite")
def test_score_pairs_cartesian_product_from_lists(test_helpers, dialect, fake_1000):
    # score_pairs returns the cartesian product of the two lists of records
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _settings())

    r1, r2 = _records()

    res = linker.inference.score_pairs([r1, r2], [r1, r2])
    assert len(res.as_record_dict()) == 4


@mark_with_dialects_excluding("sqlite")
def test_score_pairs_accepts_splink_dataframe(test_helpers, dialect, fake_1000):
    # score_pairs accepts SplinkDataFrames as well as lists of dicts
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _settings())

    r1, r2 = _records()

    sdf_left = linker._db_api.register([r1, r2], "score_pairs_left")
    sdf_right = linker._db_api.register([r1], "score_pairs_right")

    res = linker.inference.score_pairs(sdf_left, sdf_right)
    assert len(res.as_record_dict()) == 2

    # The user-supplied SplinkDataFrame must not have been mutated
    assert sdf_left.templated_name == "score_pairs_left"


@mark_with_dialects_excluding("sqlite")
def test_score_pair_rejects_list_input(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _settings())

    r1, r2 = _records()
    with pytest.raises(TypeError):
        linker.inference.score_pair([r1], r2)


@mark_with_dialects_excluding("sqlite")
def test_score_pairs_rejects_dict_input(test_helpers, dialect, fake_1000):
    helper = test_helpers[dialect]
    linker = helper.linker_with_registration(fake_1000, _settings())

    r1, r2 = _records()
    with pytest.raises(TypeError):
        linker.inference.score_pairs(r1, r2)
