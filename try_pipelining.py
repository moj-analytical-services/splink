from splink.duckdb.duckdb_linker import DuckDBInMemoryLinker
from try_settings import settings_dict
import pandas as pd
import duckdb

from try_settings import settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
# df = df.rename(columns={"group": "label"})

# con = duckdb.connect(database=":memory:")
# con.register("df", df)

# sql = """
# with __splink__df_concat as (
#     select * from df)
# select * from __splink__df_concat
# """
# con.query(sql).to_df()

linker = DuckDBInMemoryLinker(settings_dict, input_tables={"fake_data_1": df})

linker.train_u_using_random_sampling(target_rows=1e5)

blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"

linker.train_m_using_expectation_maximisation(blocking_rule)


predictions = linker.predict()

pd.DataFrame(predictions.as_record_dict())
# # linker.train_u_using_random_sampling(target_rows=1e6)


# # blocking_rule = "l.dob = r.dob"
# # linker.train_m_using_expectation_maximisation(blocking_rule)
# # linker.predict()
