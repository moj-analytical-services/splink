import datetime

import pytest

import splink.internals.comparison_library as cl
from splink import SettingsCreator
from splink.internals.blocking_rule_library import block_on

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding("sqlite")
def test_compare_two_records_1(test_helpers, dialect, fake_1000):
    # This one tests the following cases
    # - User provides a city tf tble
    # - But first_name tf table derived from input data
    helper = test_helpers[dialect]

    settings = SettingsCreator(
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

    linker = helper.linker_with_registration(fake_1000, settings)
    # linker._debug_mode = True

    city_tf = [
        {"city": "London", "tf_city": 0.2},
        {"city": "Liverpool", "tf_city": 0.8},
    ]

    linker.table_management.register_term_frequency_lookup(city_tf, "city")

    # Test with dictionary inputs
    # need to include spaces in first name as that is how they appear in csv file
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

    res = linker.inference.compare_two_records(r1, r2)
    res_dict = res.as_dict()

    # Verify term frequencies match in the comparison result
    assert res_dict["tf_city_l"][0] == 0.2
    assert res_dict["tf_city_r"][0] == 0.2
    # This is the tf value as derived from the input data
    assert pytest.approx(res_dict["tf_first_name_l"][0]) == 0.00444444444444

    assert pytest.approx(res_dict["tf_first_name_r"][0]) == 0.00444444444444


@mark_with_dialects_excluding("sqlite")
def test_compare_two_records_2(test_helpers, dialect, fake_1000):
    # This one tests the following cases
    # - User provides a city and first_name tf tables
    # - But specific values provided in input data, which take precedence

    helper = test_helpers[dialect]

    settings = SettingsCreator(
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

    linker = helper.linker_with_registration(fake_1000, settings)

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

    # Test with dictionary inputs
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

    res = linker.inference.compare_two_records(r1, r2)
    res_dict = res.as_dict()

    # Verify term frequencies match in the comparison result
    assert res_dict["tf_city_l"][0] == 0.5
    assert res_dict["tf_city_r"][0] == 0.2
    assert res_dict["tf_first_name_l"][0] == 0.3
    assert res_dict["tf_first_name_r"][0] == 0.4
