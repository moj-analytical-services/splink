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
    connection=con,
)

linker.column_frequency_chart(
    ["first_name", "surname", "first_name || surname"], top_n=2, bottom_n=3
)


# linker.train_u_using_random_sampling(target_rows=1e6)

# blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
# linker.train_m_using_expectation_maximisation(blocking_rule)

# blocking_rule = "l.dob = r.dob"
# linker.train_m_using_expectation_maximisation(blocking_rule)
# df = linker.predict()


# df.as_pandas_dataframe()

# linker.names_of_tables_created_by_splink
# linker.con.execute("pragma show_tables").fetch_df()
# linker.delete_tables_created_by_splink_from_db()
# linker.names_of_tables_created_by_splink
# linker.con.execute("pragma show_tables").fetch_df()


# cursor = linker.con.cursor()
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cursor.fetchall())
