from splink.blocking import BlockingRule, blocking_rule_to_obj
from splink.input_column import _get_dialect_quotes
from splink.settings import Settings

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_binary_composition_internals_OR(test_helpers, dialect):
    helper = test_helpers[dialect]
    brl = helper.brl

    settings = get_settings_dict()
    br_surname = brl.exact_match_rule("surname", salting_partitions=4)
    q, _ = _get_dialect_quotes(dialect)
    em_rule = f"l.{q}surname{q} = r.{q}surname{q}"
    exp_txt = "<{} blocking rule using SQL: {}>"
    assert br_surname.__repr__() == exp_txt.format("Exact match", em_rule)
    assert BlockingRule(em_rule).__repr__() == exp_txt.format("Custom", em_rule)

    assert br_surname.blocking_rule == em_rule
    assert br_surname.salting_partitions == 4
    assert br_surname.preceding_rules == []

    preceding_rules = [
        brl.exact_match_rule("first_name"),
        brl.block_on(["dob"]),
    ]
    br_surname.add_preceding_rules(preceding_rules)
    assert br_surname.preceding_rules == preceding_rules

    # Check preceding rules
    settings_tester = Settings(settings)

    brs_as_strings = [
        BlockingRule("l.help = r.help"),
        "l.help2 = r.help2",
        {"blocking_rule": "l.help3 = r.help3", "salting_partitions": 3},
        brl.exact_match_rule("help4"),
    ]
    brs_as_objs = settings_tester._brs_as_objs(brs_as_strings)
    brs_as_txt = [blocking_rule_to_obj(br).blocking_rule for br in brs_as_strings]

    assert brs_as_objs[0].preceding_rules == []

    def assess_preceding_rules(settings_brs_index):
        br_prec = brs_as_objs[settings_brs_index].preceding_rules
        br_prec_txt = [br.blocking_rule for br in br_prec]
        assert br_prec_txt == brs_as_txt[:settings_brs_index]

    assess_preceding_rules(1)
    assess_preceding_rules(2)
    assess_preceding_rules(3)


@mark_with_dialects_excluding()
def test_simple_end_to_end(test_helpers, dialect):
    helper = test_helpers[dialect]
    Linker = helper.Linker
    brl = helper.brl
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = get_settings_dict()
    settings["blocking_rules_to_generate_predictions"] = [
        brl.block_on(["first_name", "surname"]),
        brl.exact_match_rule("dob"),
    ]

    linker = Linker(df, settings, **helper.extra_linker_args())

    linker.estimate_u_using_random_sampling(target_rows=1e5)

    blocking_rule = brl.block_on(["first_name", "surname"])
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    linker.estimate_parameters_using_expectation_maximisation(
        brl.exact_match_rule("dob")
    )

    linker.predict()
