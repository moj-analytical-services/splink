from splink.duckdb.duckdb_linker import DuckDBInMemoryLinker
from try_settings import settings_dict
import pandas as pd


from try_settings import settings_dict


df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

df = df.reset_index()
df["side"] = df.index % 2

df_left = df[df["side"] == 0]
df_right = df[df["side"] == 1]

settings_dict["link_type"] = "link_only"

linker = DuckDBInMemoryLinker(
    settings_dict, input_tables={"df_left": df_left, "df_right": df_right}
)

linker.train_u_using_random_sampling(target_rows=1e6)

blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
linker.train_m_using_expectation_maximisation(blocking_rule)

blocking_rule = "l.dob = r.dob"
linker.train_m_using_expectation_maximisation(blocking_rule)
df = linker.predict()
df.as_pandas_dataframe()


# df_pd = df.as_pandas_dataframe()
# df_pd.sort_values(["unique_id_l", "unique_id_r"])
# df_new["source_dataset"] = "df_new"


# linker.con.register("__splink__new", df_new)


# linker.compute_tf_table("first_name")
# linker.compute_tf_table("city")


# linker.con.execute(sql)
# print(linker.con.fetchall())
# import time

# start_time = time.time()

# df = linker.incremental_link("__splink__new").as_pandas_dataframe()
# df
# print("--- %s seconds ---" % (time.time() - start_time))
# linker.list_tables()
