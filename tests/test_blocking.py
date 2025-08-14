from splink.internals.blocking import BlockingRule, blocking_rule_to_obj
from splink.internals.blocking_rule_library import block_on
from splink.internals.linker import Linker

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_match_keys(dialect):
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

    BlockingRule._set_match_keys_for_each_blocking_rule(blocking_rules)

    for n, br in enumerate(blocking_rules):
        assert br.match_key == n


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
