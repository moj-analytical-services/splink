from splink.internals.blocking import BlockingRule, blocking_rule_to_obj
from splink.internals.blocking_rule_library import block_on
from splink.internals.input_column import _get_dialect_quotes
from splink.internals.linker import Linker

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_preceding_blocking_rules(dialect):
    br_surname = block_on("surname", salting_partitions=4).get_blocking_rule(dialect)

    q, _ = _get_dialect_quotes(dialect)
    em_rule = f"l.{q}surname{q} = r.{q}surname{q}"

    assert br_surname.blocking_rule_sql == em_rule
    assert br_surname.salting_partitions == 4
    assert br_surname.preceding_rules == []

    preceding_rules = [
        block_on("first_name").get_blocking_rule(dialect),
        block_on("dob").get_blocking_rule(dialect),
    ]
    br_surname.add_preceding_rules(preceding_rules)
    assert br_surname.preceding_rules == preceding_rules

    blocking_rules = [
        BlockingRule("l.help = r.help", dialect),
        blocking_rule_to_obj(
            {"blocking_rule": "l.help2 = r.help2", "sql_dialect": dialect}
        ),
        blocking_rule_to_obj(
            {
                "blocking_rule": "l.help3 = r.help3",
                "salting_partitions": 3,
                "sql_dialect": dialect,
            }
        ),
        block_on("help4").get_blocking_rule(dialect),
    ]
    blocking_rules = BlockingRule._add_preceding_rules_to_each_blocking_rule(
        blocking_rules
    )
    blocking_rule_sqls = [
        blocking_rule_to_obj(br).blocking_rule_sql for br in blocking_rules
    ]

    assert blocking_rules[0].preceding_rules == []

    def assess_preceding_rules(settings_brs_index):
        br_prec = blocking_rules[settings_brs_index].preceding_rules
        br_prec_txt = [br.blocking_rule_sql for br in br_prec]
        assert br_prec_txt == blocking_rule_sqls[:settings_brs_index]

    assess_preceding_rules(1)
    assess_preceding_rules(2)
    assess_preceding_rules(3)


@mark_with_dialects_excluding()
def test_simple_end_to_end(test_helpers, dialect):
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = get_settings_dict()
    settings["blocking_rules_to_generate_predictions"] = [
        block_on("first_name", "surname"),
        block_on("dob"),
    ]

    linker = Linker(df, settings, **helper.extra_linker_args())

    linker.training.estimate_u_using_random_sampling(max_pairs=1e3)

    blocking_rule = block_on("first_name", "surname")
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

    linker.inference.predict()
