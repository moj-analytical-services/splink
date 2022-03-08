from splink.mssql.mssql_linker import MSSQLLinker, MSSQLConnection
from try_settings import settings_dict
import pandas as pd

from try_settings import settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

df = df.reset_index()
df["side"] = df.index % 2

df_left = df[df["side"] == 0]
df_right = df[df["side"] == 1]

settings_dict["link_type"] = "link_only"

ms_con = MSSQLConnection(
    server = "127.0.0.1",
    database = 'TestEnv',
    username = 'sa',
    password = 'qq5SHUz8jLtpDM82'
)

linker = MSSQLLinker(
    settings_dict, ms_con, input_tables={"df_left": df_left, "df_right": df_right}
)

linker.train_u_using_random_sampling(target_rows=1e6)
blocking_rule = "l.surname = r.surname"
linker.train_m_using_expectation_maximisation(blocking_rule)
blocking_rule = "l.dob = r.dob"
linker.train_m_using_expectation_maximisation(blocking_rule)

df = linker.predict()
df.as_pandas_dataframe()
