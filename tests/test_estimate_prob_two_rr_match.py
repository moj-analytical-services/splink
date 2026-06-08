import logging

import pytest

from splink import block_on
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker

from .decorator import mark_with_dialects_excluding


def test_prob_rr_match_sampled_probe_is_similar_to_exact(fake_1000):
    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": ["l.first_name = r.first_name"],
        "comparisons": [],
    }
    deterministic_rules = [block_on("first_name")]

    exact_db_api = DuckDBAPI()
    exact_sdf = exact_db_api.register(fake_1000)
    exact_linker = Linker(exact_sdf, settings)
    exact_linker.training.estimate_probability_two_random_records_match(
        deterministic_rules,
        recall=1.0,
        probe_proportion=1.0,
    )
    exact_prob = exact_linker._settings_obj._probability_two_random_records_match

    sampled_db_api = DuckDBAPI()
    sampled_sdf = sampled_db_api.register(fake_1000)
    sampled_linker = Linker(sampled_sdf, settings)
    with pytest.warns(
        UserWarning,
        match="below the recommended minimum of 1,000",
    ):
        sampled_linker.training.estimate_probability_two_random_records_match(
            deterministic_rules,
            recall=1.0,
            probe_proportion=0.5,
        )
    sampled_prob = sampled_linker._settings_obj._probability_two_random_records_match

    assert sampled_prob == pytest.approx(exact_prob, rel=0.2)


@mark_with_dialects_excluding()
def test_prob_rr_match_dedupe(test_helpers, dialect):
    helper = test_helpers[dialect]
    data = [
        {"unique_id": 1, "first_name": "John", "surname": "Smith"},
        {"unique_id": 2, "first_name": "John", "surname": "Smith"},
        {"unique_id": 3, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 4, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 5, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 6, "first_name": "Jane", "surname": "Taylor"},
    ]

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [],
    }

    deterministic_rules = ["l.first_name = r.first_name", "l.surname = r.surname"]

    # Test dedupe only
    linker = helper.linker_with_registration([data], settings)
    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules, recall=1.0
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 4 matches and 15 comparisons
    assert pytest.approx(prob) == 4 / 15

    # Test recall works
    deterministic_rules = ["l.first_name = r.first_name and l.surname = r.surname"]
    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules, recall=0.9
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 4 matches and 15 comparisons
    assert pytest.approx(prob) == 4 / 15 * (1 / 0.9)


@mark_with_dialects_excluding()
def test_prob_rr_match_link_only(test_helpers, dialect):
    helper = test_helpers[dialect]
    data_1 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smith"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
    ]

    data_2 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
        {"unique_id": 4, "first_name": "Alice", "surname": "Williams"},
    ]

    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [],
    }

    deterministic_rules = ["l.first_name = r.first_name", "l.surname = r.surname"]

    # Test dedupe only
    linker = helper.linker_with_registration([data_1, data_2], settings)
    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules, recall=1.0
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 2 matches and 8 comparisons
    assert pytest.approx(prob) == 2 / 8


@mark_with_dialects_excluding()
def test_prob_rr_match_link_and_dedupe(test_helpers, dialect):
    helper = test_helpers[dialect]
    data_1 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smith"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jane", "surname": "Tailor"},
    ]

    data_2 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
    ]

    settings = {
        "link_type": "link_and_dedupe",
        "blocking_rules_to_generate_predictions": ["1=1"],
        "comparisons": [],
    }

    deterministic_rules = ["l.first_name = r.first_name", "l.surname = r.surname"]

    # Test dedupe only
    linker = helper.linker_with_registration([data_1, data_2], settings)
    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules, recall=1.0
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 3 matches and 15 comparisons
    assert pytest.approx(prob) == 3 / 15


@mark_with_dialects_excluding()
def test_prob_rr_match_link_only_multitable(test_helpers, dialect):
    helper = test_helpers[dialect]
    data_1 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smith"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Hannah", "surname": "Jones"},
    ]

    data_2 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
        {"unique_id": 4, "first_name": "Alice", "surname": "Williams"},
    ]

    data_3 = [
        {"unique_id": 1, "first_name": "Graham", "surname": "Roberts"},
        {"unique_id": 2, "first_name": "Graham", "surname": "Robinson"},
        {"unique_id": 3, "first_name": "Mary", "surname": "Taylor"},
        {"unique_id": 4, "first_name": "Graham", "surname": "Roberts"},
        {"unique_id": 5, "first_name": "Sarah", "surname": "Thompson"},
    ]

    data_4 = [
        {"unique_id": 1, "first_name": "Johnny", "surname": "Brown"},
        {"unique_id": 2, "first_name": "Ben", "surname": "Davies"},
        {"unique_id": 3, "first_name": "Felicity", "surname": "Wright"},
        {"unique_id": 4, "first_name": "Kelly", "surname": "Evans"},
        {"unique_id": 5, "first_name": "David", "surname": "Thomas"},
        {"unique_id": 6, "first_name": "Bryan", "surname": "Wilson"},
        {"unique_id": 7, "first_name": "Brian", "surname": "Johnson"},
    ]
    datas = list(
        map(
            lambda data: list(map(lambda row: {**row, "city": "Brighton"}, data)),
            (data_1, data_2, data_3, data_4),
        )
    )

    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [],
    }

    deterministic_rules = ["l.first_name = r.first_name", "l.surname = r.surname"]

    linker = helper.linker_with_registration(datas, settings)
    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules, recall=1.0
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 6 matches (1 John, 3 Mary, 1 Jones (ignoring already matched Mary), 1 Taylor)
    # 4*3 + 4*5 + 4*7 + 3*5 + 3*7 + 5*7 = 131 comparisons
    assert pytest.approx(prob) == 6 / 131

    # if we define all record pairs to be a match, then the probability should be 1
    linker = helper.linker_with_registration(datas, settings)
    linker.training.estimate_probability_two_random_records_match(
        ["l.city = r.city"], recall=1.0
    )
    prob = linker._settings_obj._probability_two_random_records_match
    assert prob == 1


@mark_with_dialects_excluding()
def test_prob_rr_match_link_and_dedupe_multitable(test_helpers, dialect):
    helper = test_helpers[dialect]
    data_1 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smith"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Hannah", "surname": "Jones"},
    ]

    data_2 = [
        {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
        {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
        {"unique_id": 4, "first_name": "Alice", "surname": "Williams"},
    ]

    data_3 = [
        {"unique_id": 1, "first_name": "Graham", "surname": "Roberts"},
        {"unique_id": 2, "first_name": "Graham", "surname": "Robinson"},
        {"unique_id": 3, "first_name": "Mary", "surname": "Taylor"},
        {"unique_id": 4, "first_name": "Graham", "surname": "Roberts"},
        {"unique_id": 5, "first_name": "Sarah", "surname": "Thompson"},
    ]

    data_4 = [
        {"unique_id": 1, "first_name": "Johnny", "surname": "Brown"},
        {"unique_id": 2, "first_name": "Ben", "surname": "Davies"},
        {"unique_id": 3, "first_name": "Felicity", "surname": "Wright"},
        {"unique_id": 4, "first_name": "Kelly", "surname": "Evans"},
        {"unique_id": 5, "first_name": "David", "surname": "Thomas"},
        {"unique_id": 6, "first_name": "Bryan", "surname": "Wilson"},
        {"unique_id": 7, "first_name": "Brian", "surname": "Johnson"},
    ]
    datas = list(
        map(
            lambda data: list(map(lambda row: {**row, "city": "Brighton"}, data)),
            (data_1, data_2, data_3, data_4),
        )
    )

    settings = {
        "link_type": "link_and_dedupe",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [],
    }

    deterministic_rules = ["l.first_name = r.first_name", "l.surname = r.surname"]

    linker = helper.linker_with_registration(datas, settings)
    linker.training.estimate_probability_two_random_records_match(
        deterministic_rules, recall=1.0
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 10 matches (1 John, 3 Mary, 2 Jones (ignoring already matched Mary),
    # 1 Taylor, 3 Graham, 0 Roberts (ignoring already counted Graham))
    # (3 + 4 + 5 + 7)(3 + 4 + 5 + 7 - 1)/2 = 171 comparisons
    assert pytest.approx(prob) == 10 / 171

    linker = helper.linker_with_registration(datas, settings)
    linker.training.estimate_probability_two_random_records_match(
        ["l.city = r.city"], recall=1.0
    )
    prob = linker._settings_obj._probability_two_random_records_match
    assert prob == 1


@mark_with_dialects_excluding()
def test_prob_rr_valid_range(test_helpers, dialect, caplog):
    helper = test_helpers[dialect]

    def check_range(p):
        assert p <= 1
        assert p >= 0

    data = [
        {
            "unique_id": 1,
            "first_name": "John",
            "surname": "Smith",
            "city": "Brighton",
        },
        {
            "unique_id": 2,
            "first_name": "John",
            "surname": "Williams",
            "city": "Brighton",
        },
        {
            "unique_id": 3,
            "first_name": "John",
            "surname": "Jones",
            "city": "Brighton",
        },
        {
            "unique_id": 4,
            "first_name": "John",
            "surname": "Davis",
            "city": "Swansea",
        },
        {
            "unique_id": 5,
            "first_name": "John",
            "surname": "Evans",
            "city": "Swansea",
        },
        {
            "unique_id": 6,
            "first_name": "John",
            "surname": "Wright",
            "city": "Swansea",
        },
    ]

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [],
    }

    # Test dedupe only
    linker = helper.linker_with_registration([data], settings)
    with pytest.raises(ValueError):
        # all comparisons matches using this rule, so we must have perfect recall
        # using recall = 80% is inconsistent, so should get an error
        linker.training.estimate_probability_two_random_records_match(
            ["l.first_name = r.first_name"], recall=0.8
        )
    check_range(linker._settings_obj._probability_two_random_records_match)

    # matching on city gives 6 matches out of 15, so recall must be at least 6/15
    recall_min_city = 6 / 15
    with pytest.raises(ValueError):
        linker.training.estimate_probability_two_random_records_match(
            ["l.city = r.city"], recall=(recall_min_city - 1e-6)
        )
    linker.training.estimate_probability_two_random_records_match(
        ["l.city = r.city"], recall=recall_min_city
    )
    check_range(linker._settings_obj._probability_two_random_records_match)

    # no comparisons matches using this rule, so we will estimate value as 0
    # this gives a linkage model that always predicts match_probability as 0,
    # so should give a warning at this stage
    with caplog.at_level(logging.WARNING):
        linker.training.estimate_probability_two_random_records_match(
            ["l.surname = r.surname"], recall=0.7
        )
        assert "WARNING:" in caplog.text
    check_range(linker._settings_obj._probability_two_random_records_match)

    # this gives prob as 1, so again should get a warning
    # as we have a trivial linkage model
    with caplog.at_level(logging.WARNING):
        linker.training.estimate_probability_two_random_records_match(
            ["l.first_name = r.first_name"], recall=1.0
        )
        assert "WARNING:" in caplog.text
    check_range(linker._settings_obj._probability_two_random_records_match)

    # check we get errors if we pass bogus values for recall
    with pytest.raises(ValueError):
        linker.training.estimate_probability_two_random_records_match(
            ["l.first_name = r.first_name"], recall=0.0
        )
    with pytest.raises(ValueError):
        linker.training.estimate_probability_two_random_records_match(
            ["l.first_name = r.first_name"], recall=1.2
        )
    with pytest.raises(ValueError):
        linker.training.estimate_probability_two_random_records_match(
            ["l.first_name = r.first_name"], recall=-0.4
        )
