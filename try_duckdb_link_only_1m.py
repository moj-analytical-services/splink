from splink.duckdb.duckdb_linker import DuckDBInMemoryLinker
from try_settings import settings_dict
import pandas as pd


from try_settings import settings_dict


df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
df = pd.read_csv("./benchmarking/fake_20000_from_splink_demos.csv")

df_main = df
df_new = df[:1].copy()

linker = DuckDBInMemoryLinker(settings_dict, input_tables={"main": df_main})


# Train it as a dedupe job.
# If you were to do that, the left hand table would be '__splink__df_concat_with_tf'

# It needs a 'link_incremental' method that treats ''__splink__df_concat_with_tf'' as the left
# table and 'main' as the right table of a link_only.


# linker.list_tables()
linker.train_u_using_random_sampling(target_rows=1e6)

blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
linker.train_m_using_expectation_maximisation(blocking_rule)

blocking_rule = "l.dob = r.dob"
linker.train_m_using_expectation_maximisation(blocking_rule)
df = linker.predict()


df_pd = df.as_pandas_dataframe()
df_pd.sort_values(["unique_id_l", "unique_id_r"])
df_new["source_dataset"] = "df_new"


linker.con.register("__splink__new", df_new)


linker.compute_tf_table("first_name")
linker.compute_tf_table("city")

# Probably no need for this, they're created automatically.
sql = """
CREATE INDEX first_name_idx ON __splink__df_concat_with_tf (first_name);
CREATE INDEX surname_idx ON __splink__df_concat_with_tf (surname);
CREATE INDEX dob_idx ON __splink__df_concat_with_tf (dob);

CREATE INDEX tf_first_name_idx ON __splink__df_tf_first_name (first_name);
CREATE INDEX tf_city_idx ON __splink__df_tf_city (city);

"""
linker.con.execute(sql)
print(linker.con.fetchall())
import time

start_time = time.time()

df = linker.incremental_link("__splink__new").as_pandas_dataframe()
df
print("--- %s seconds ---" % (time.time() - start_time))
linker.list_tables()
