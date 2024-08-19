import logging
import re

import pandas as pd

import splink.internals.comparison_library as cl
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker


# Create a log handler that allows us to captured logged messages to a python list
class ListHandler(logging.Handler):
    def __init__(self, log_list):
        super().__init__()
        self.log_list = log_list

    def emit(self, record):
        self.log_list.append(self.format(record))


logger = logging.getLogger("splink")


data_one = [
    {
        "unique_id": "a0",
        "first_name": "Julia ",
        "surname": "Taylor",
        "dob": "2015-07-31",
        "city": "London",
        "email": "hannah88@powers.com",
        "cluster": "0",
    },
    {
        "unique_id": "a1",
        "first_name": "Juli",
        "surname": "Taylor",
        "dob": "2015-07-31",
        "city": "London",
        "email": "hannah8@powers.com",
        "cluster": "0",
    },
    {
        "unique_id": "a2",
        "first_name": "Julia ",
        "surname": "Taylo",
        "dob": "2015-07-31",
        "city": "Lambeth",
        "email": "hannah88@powers.com",
        "cluster": "0",
    },
    {
        "unique_id": "a3",
        "first_name": "Juli",
        "surname": "Taylor",
        "dob": "2015-07-31",
        "city": "Lambeth",
        "email": "hannah88@powers.com",
        "cluster": "0",
    },
]

data_three = [
    {
        "unique_id": "b0",
        "first_name": "Juli",
        "surname": "Taylo",
        "dob": "2015-07-31",
        "city": "London",
        "email": "hannah88@powers.com",
        "cluster": "0",
    },
    {
        "unique_id": "b1",
        "first_name": "Juli",
        "surname": "Taylr",
        "dob": "2015-07-31",
        "city": "London",
        "email": "hannah8@powers.com",
        "cluster": "0",
    },
    {
        "unique_id": "b2",
        "first_name": "Julia",
        "surname": "Taylor",
        "dob": "2015-07-31",
        "city": "Lambeth",
        "email": "hannah88@powers.com",
        "cluster": "0",
    },
]


def test_dedupe_only():
    df_one = pd.DataFrame(data_one)

    log_list = []
    handler = ListHandler(log_list)
    logger.addHandler(handler)

    settings = {
        "link_type": "dedupe_only",
        "probability_two_random_records_match": 4 / 1000,
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.ExactMatch("email"),
        ],
    }
    db_api = DuckDBAPI()
    linker = Linker(
        df_one,
        settings,
        db_api=db_api,
        set_up_basic_logging=False,
    )
    logging.getLogger("splink").setLevel(1)

    linker.training.estimate_u_using_random_sampling(max_pairs=1000)
    all_log_messages = "\n".join(log_list)
    all_log_messages = re.sub(r"\s+", " ", all_log_messages)

    assert (
        "from __splink__df_concat_sample as l inner join __splink__df_concat_sample as r"  # noqa: E501
        in all_log_messages
    )

    handler.log_list.clear()

    linker.inference.predict()

    all_log_messages = "\n".join(log_list)
    all_log_messages = re.sub(r"\s+", " ", all_log_messages)

    assert (
        "from __splink__df_concat_with_tf as l inner join __splink__df_concat_with_tf as r"  # noqa: E501
        in all_log_messages
    )


def test_link_and_dedupe():
    df_one = pd.DataFrame(data_one)
    df_two = pd.read_csv("tests/datasets/fake_1000_from_splink_demos.csv")

    log_list = []
    handler = ListHandler(log_list)
    logger.addHandler(handler)
    settings = {
        "link_type": "link_and_dedupe",
        "probability_two_random_records_match": 4 / 1000,
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.ExactMatch("email"),
        ],
    }
    db_api = DuckDBAPI()
    linker = Linker(
        [df_one, df_two],
        settings,
        db_api=db_api,
        input_table_aliases=["df_one", "df_two"],
        set_up_basic_logging=False,
    )

    handler.log_list.clear()
    logging.getLogger("splink").setLevel(1)
    linker.training.estimate_u_using_random_sampling(max_pairs=1000)

    all_log_messages = "\n".join(log_list)
    all_log_messages = re.sub(r"\s+", " ", all_log_messages)
    assert (
        "from __splink__df_concat_sample as l inner join __splink__df_concat_sample as r"  # noqa: E501
        in all_log_messages
    )

    log_list.clear()

    linker.inference.predict()

    all_log_messages = "\n".join(log_list)
    all_log_messages = re.sub(r"\s+", " ", all_log_messages)

    assert (
        "from __splink__df_concat_with_tf as l inner join __splink__df_concat_with_tf as r"  # noqa: E501
        in all_log_messages
    )


def test_link_only_two():
    df_one = pd.DataFrame(data_one)
    df_two = pd.read_csv("tests/datasets/fake_1000_from_splink_demos.csv")

    log_list = []
    handler = ListHandler(log_list)
    logger.addHandler(handler)

    settings = {
        "link_type": "link_only",
        "probability_two_random_records_match": 4 / 1000,
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.ExactMatch("email"),
        ],
    }
    db_api = DuckDBAPI()
    linker = Linker(
        [df_one, df_two],
        settings,
        db_api=db_api,
        input_table_aliases=["df_one", "df_two"],
        set_up_basic_logging=False,
    )

    log_list.clear()
    logging.getLogger("splink").setLevel(1)
    linker.training.estimate_u_using_random_sampling(max_pairs=1000)

    all_log_messages = "\n".join(log_list)
    all_log_messages = re.sub(r"\s+", " ", all_log_messages)
    assert (
        "from __splink__df_concat_sample_left as l inner join __splink__df_concat_sample_right as r"  # noqa: E501
        in all_log_messages
    )

    log_list.clear()

    linker.inference.predict()

    all_log_messages = "\n".join(log_list)
    all_log_messages = re.sub(r"\s+", " ", all_log_messages)

    assert (
        "from __splink__df_concat_with_tf_left as l inner join __splink__df_concat_with_tf_right as r"  # noqa: E501
        in all_log_messages
    )


def test_link_only_three():
    df_one = pd.DataFrame(data_one)
    df_two = pd.read_csv("tests/datasets/fake_1000_from_splink_demos.csv")
    df_three = pd.DataFrame(data_three)

    log_list = []
    handler = ListHandler(log_list)
    logger.addHandler(handler)

    settings = {
        "link_type": "link_only",
        "probability_two_random_records_match": 4 / 1000,
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.ExactMatch("email"),
        ],
    }
    db_api = DuckDBAPI()
    linker = Linker(
        [df_one, df_two, df_three],
        settings,
        db_api=db_api,
        input_table_aliases=["df_one", "df_two", "df_three"],
        set_up_basic_logging=False,
    )

    log_list.clear()
    logging.getLogger("splink").setLevel(1)
    linker.training.estimate_u_using_random_sampling(max_pairs=1000)

    all_log_messages = "\n".join(log_list)
    all_log_messages = re.sub(r"\s+", " ", all_log_messages)
    assert (
        "from __splink__df_concat_sample as l inner join __splink__df_concat_sample as r"  # noqa: E501
        in all_log_messages
    )

    log_list.clear()

    linker.inference.predict()

    all_log_messages = "\n".join(log_list)
    all_log_messages = re.sub(r"\s+", " ", all_log_messages)

    assert (
        "from __splink__df_concat_with_tf as l inner join __splink__df_concat_with_tf as r"  # noqa: E501
        in all_log_messages
    )
