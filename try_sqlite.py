from splink.sqlite.sqlite_linker import SQLiteLinker
from try_settings import settings_dict
import pandas as pd
import sqlite3


con = sqlite3.connect(":memory:")
df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
# df = pd.read_csv("./benchmarking/fake_20000_from_splink_demos.csv")
df.to_sql("input_df_tablename", con)

# sql = f"PRAGMA table_info('input_df_tadblename');"
# con.execute(sql).fetchone()
# # sql = "create table input_df_tablename as  select * from input_df_tablename"
# # pd.read_sql(sql,con)

linker = SQLiteLinker(
    settings_dict,
    input_tables={"mydf": "input_df_tablename"},
    sqlite_connection=con,
)

linker.train_u_using_random_sampling(target_rows=1e6)

blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
linker.train_m_using_expectation_maximisation(blocking_rule)

blocking_rule = "l.dob = r.dob"
linker.train_m_using_expectation_maximisation(blocking_rule)
df = linker.predict()


df.as_pandas_dataframe()
df_pd.sort_values(["unique_id_l", "unique_id_r"])
