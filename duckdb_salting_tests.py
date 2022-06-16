import os

os.chdir("/Users/thomashepworth/py-data-linking/splink3")

from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd

from tests.basic_settings import get_settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

settings = get_settings_dict()
settings["salting"] = 10
# settings["salting"] = 1
settings["link_type"] = "link_and_dedupe"

linker = DuckDBLinker(
    [df, df],
    settings,
    output_schema="splink_in_duckdb",
    input_table_aliases="testing",
)

# linker._settings_dict
# linker._initialise_df_concat_with_tf(materialise=True)

linker.estimate_u_using_random_sampling(target_rows=1e6)

print("==========================================================")
print("Estimating params using expectation maximisation")
print("==========================================================")

blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

blocking_rule = "l.dob = r.dob"
linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

df_predict = linker.predict()
display(df_predict.as_pandas_dataframe().sort_values(by=["unique_id_l", "unique_id_r"]))
# df_predict.as_pandas_dataframe()

# linker._con.execute("select * from __splink__df_concat_with_tf").fetch_df()
# linker._con.execute("select * from __splink__df_concat_with_tf").fetch_df().value_counts("__splink_salt")

# sql = """
# WITH __splink__df_concat AS (
#     SELECT
#       unique_id,
#       first_name,
#       surname,
#       dob,
#       city,
#       email,
#       "group"
#     FROM testing
# ), __splink__df_tf_first_name AS (
#     SELECT
#       first_name,
#       CAST(COUNT(*) AS DOUBLE) / (SELECT COUNT(first_name) AS total FROM __splink__df_concat) AS tf_first_name
#     FROM __splink__df_concat
#     WHERE
#       first_name IS NOT NULL
#     GROUP BY
#       first_name
# ), __splink__df_concat_with_tf AS (
#     SELECT
#       __splink__df_concat.*,
#       __splink__df_tf_first_name.tf_first_name,
#       CEIL(RANDOM() * 2) AS __splink_salt
#     FROM __splink__df_concat
#     LEFT JOIN __splink__df_tf_first_name
#       ON __splink__df_concat.first_name = __splink__df_tf_first_name.first_name
# ), __splink__df_blocked AS (
#     SELECT
#       l.unique_id AS unique_id_l,
#       r.unique_id AS unique_id_r,
#       l.first_name AS first_name_l,
#       r.first_name AS first_name_r,
#       l.tf_first_name AS tf_first_name_l,
#       r.tf_first_name AS tf_first_name_r,
#       l.surname AS surname_l,
#       r.surname AS surname_r,
#       l.dob AS dob_l,
#       r.dob AS dob_r,
#       l.email AS email_l,
#       r.email AS email_r,
#       l.city AS city_l,
#       r.city AS city_r,
#       l."group" AS "group_l",
#       r."group" AS "group_r",
#       '0' AS match_key
#     FROM __splink__df_concat_with_tf AS l
#     INNER JOIN __splink__df_concat_with_tf AS r
#       ON l.surname = r.surname
#       AND l.__splink_salt = 1
#     WHERE
#       l.unique_id < r.unique_id
#     UNION ALL
#     SELECT
#       l.unique_id AS unique_id_l,
#       r.unique_id AS unique_id_r,
#       l.first_name AS first_name_l,
#       r.first_name AS first_name_r,
#       l.tf_first_name AS tf_first_name_l,
#       r.tf_first_name AS tf_first_name_r,
#       l.surname AS surname_l,
#       r.surname AS surname_r,
#       l.dob AS dob_l,
#       r.dob AS dob_r,
#       l.email AS email_l,
#       r.email AS email_r,
#       l.city AS city_l,
#       r.city AS city_r,
#       l."group" AS "group_l",
#       r."group" AS "group_r",
#       '0' AS match_key
#     FROM __splink__df_concat_with_tf AS l
#     INNER JOIN __splink__df_concat_with_tf AS r
#       ON l.surname = r.surname
#       AND l.__splink_salt = 2
#     WHERE
#       l.unique_id < r.unique_id
#       )
# SELECT * from __splink__df_blocked
# """

# linker._con.execute(sql).fetch_df()


# sql = """
# WITH __splink__df_concat AS (
#     SELECT
#       unique_id,
#       first_name,
#       surname,
#       dob,
#       city,
#       email,
#       "group"
#     FROM testing
# ), __splink__df_tf_first_name AS (
#     SELECT
#       first_name,
#       CAST(COUNT(*) AS DOUBLE) / (SELECT COUNT(first_name) AS total FROM __splink__df_concat) AS tf_first_name
#     FROM __splink__df_concat
#     WHERE
#       first_name IS NOT NULL
#     GROUP BY
#       first_name
# ), __splink__df_concat_with_tf AS (
#     SELECT
#       __splink__df_concat.*,
#       __splink__df_tf_first_name.tf_first_name,
#       CEIL(RANDOM() * 2) AS __splink_salt
#     FROM __splink__df_concat
#     LEFT JOIN __splink__df_tf_first_name
#       ON __splink__df_concat.first_name = __splink__df_tf_first_name.first_name
# ), __splink__df_blocked AS (
#     SELECT
#       l.unique_id AS unique_id_l,
#       r.unique_id AS unique_id_r,
#       l.first_name AS first_name_l,
#       r.first_name AS first_name_r,
#       l.tf_first_name AS tf_first_name_l,
#       r.tf_first_name AS tf_first_name_r,
#       l.surname AS surname_l,
#       r.surname AS surname_r,
#       l.dob AS dob_l,
#       r.dob AS dob_r,
#       l.email AS email_l,
#       r.email AS email_r,
#       l.city AS city_l,
#       r.city AS city_r,
#       l."group" AS "group_l",
#       r."group" AS "group_r",
#       '0' AS match_key
#     FROM __splink__df_concat_with_tf AS l
#     INNER JOIN __splink__df_concat_with_tf AS r
#       ON l.surname = r.surname
#     WHERE
#       l.unique_id < r.unique_id
#       )
# SELECT * from __splink__df_blocked
# """


# linker._con.execute(sql).fetch_df()

# linker._con.execute("select * from __splink__df_concat_with_tf").fetch_df().value_counts("__splink_salt")
