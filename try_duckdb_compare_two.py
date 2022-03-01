from splink.duckdb.duckdb_linker import DuckDBLinker
from try_settings import settings_dict
import pandas as pd


from try_settings import settings_dict


df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")


linker = DuckDBLinker(
    settings_dict, input_tables={"input_df": df}, connection=":temporary:"
)

linker.train_u_using_random_sampling(target_rows=1e6)

blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
linker.train_m_using_expectation_maximisation(blocking_rule)

blocking_rule = "l.dob = r.dob"
linker.train_m_using_expectation_maximisation(blocking_rule)


linker.compute_tf_table("first_name")
linker.compute_tf_table("city")


record_1 = {
    "unique_id": 0,
    "first_name": "Jules ",
    "surname": None,
    "dob": "2015-10-29",
    "city": "London",
    "email": "hannah88@powers.com",
    "group": 0,
}

record_2 = {
    "unique_id": 1,
    "first_name": "Julia ",
    "surname": "Taylor",
    "dob": "2015-07-31",
    "city": "London",
    "email": "hannah88@powers.com",
    "group": 0,
}

linker.compare_two_records(record_1, record_2).as_pandas_dataframe()
