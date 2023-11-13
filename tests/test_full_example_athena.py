# import os

# import pandas as pd
# import pytest

# import splink.comparison_library as cl

# from .basic_settings import get_settings_dict
# from .linker_utils import _test_table_registration

# # Skip if no valid boto3 connection exists
# try:
#     import boto3

#     # Check if a valid s3 connection is available
#     sts_client = boto3.client("sts")
#     response = sts_client.get_caller_identity()
#     import awswrangler as wr

#     from splink.athena.athena_utils import athena_warning_text
#     from splink.athena.linker import AthenaLinker
# except ImportError:
#     # Skip if no AWS Connection exists
#     pytestmark = pytest.mark.skip(reason="AWS Connection Required")

# TODO: this is all commented out until we have the functionality, as it errors at
# top-level currently
# settings_dict = get_settings_dict()

# first_name_cc = cl.LevenshteinAtThresholds(
#     col_name="first_name",
#     distance_threshold_or_thresholds=2,
# TODO: compat
# include_exact_match_level=True,
# term_frequency_adjustments=True,
# m_probability_exact_match=0.7,
# m_probability_or_probabilities_lev=0.2,
# m_probability_else=0.1,
# )


# # dob_cc = cl.datediff_at_thresholds(
# #     col_name="dob",
# #     date_thresholds=[7, 3, 1],
# #     date_metrics=["day", "month", "year"],
# #     cast_strings_to_date=True,
# # )

# # Update tf weight and u probabilities to match
# first_name_cc._comparison_dict["comparison_levels"][1]._tf_adjustment_weight = 0.6
# u_probabilities_first_name = [0.1, 0.1, 0.8]
# for u_prob, level in zip(
#     u_probabilities_first_name,
#     first_name_cc._comparison_dict["comparison_levels"][1:],
# ):
#     level._u_probability = u_prob

# # Update settings w/ our edited first_name col
# settings_dict["comparisons"][0] = first_name_cc
# # settings_dict["comparisons"][2] = dob_cc

# # Setup database names for tests
# db_name_read = "splink_awswrangler_test"
# db_name_write = "data_linking_temp"
# output_bucket = "alpha-data-linking"
# table_name = "__splink__fake_1000_from_splink_demos"


# def upload_data(db_name):
#     df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
#     bucket = output_bucket
#     # ensure data is in a unique loc to __splink tables
#     path = f"s3://{bucket}/data/"

#     path = f"s3://{bucket}/data/{table_name}"
#     wr.s3.to_parquet(
#         df=df,
#         path=path,
#         dataset=True,
#         mode="overwrite",
#         database=db_name,
#         table=table_name,
#         compression="snappy",
#     )


# def test_full_example_athena(tmp_path):
#     # This test assumes the databases in use have already been created

#     # creates a session at least on the platform...
#     my_session = boto3.Session(region_name="eu-west-1")

#     # Upload our raw data
#     upload_data(db_name_read)

#     linker = AthenaLinker(
#         settings_dict=settings_dict,
#         input_table_or_tables=f"{db_name_read}.{table_name}",
#         boto3_session=my_session,
#         output_bucket=output_bucket,
#         output_database=db_name_write,
#         output_filepath="athena_test_full_example",
#     )

#     linker.profile_columns(
#         [
#             "first_name",
#             "surname",
#             "first_name || surname",
#             "concat(city, first_name)",
#             ["surname", "city"],
#         ]
#     )
#     linker.compute_tf_table("city")
#     linker.compute_tf_table("first_name")

#     linker.estimate_u_using_random_sampling(max_pairs=1e6, seed=None)

#     blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
#     linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

#     blocking_rule = "l.dob = r.dob"
#     linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

#     df_predict = linker.predict()

#     linker.comparison_viewer_dashboard(df_predict, "test_scv_athena.html", True, 2)

#     df_predict.as_pandas_dataframe()

#     df_clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.1)

#     linker.cluster_studio_dashboard(
#         df_predict,
#         df_clusters,
#         sampling_method="by_cluster_size",
#         out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
#     )

#     linker.unlinkables_chart(source_dataset="Testing")

#     _test_table_registration(linker, skip_dtypes=True)

#     # Clean up our database and s3 bucket and remove test files
#     linker.drop_all_tables_created_by_splink(delete_s3_folders=True)
#     linker.drop_splink_tables_from_database(database_name=db_name_read)


# def test_athena_garbage_collection():
#     # creates a session at least on the platform...
#     my_session = boto3.Session(region_name="eu-west-1")
#     upload_data(db_name_read)

#     out_fp = "athena_test_garbage_collection"

#     def run_athena_predictions():
#         # Test that our gc works as expected w/ tables_to_exclude
#         linker = AthenaLinker(
#             settings_dict=settings_dict,
#             input_table_or_tables=f"{db_name_read}.{table_name}",
#             boto3_session=my_session,
#             output_bucket=output_bucket,
#             output_database=db_name_write,
#             output_filepath=out_fp,
#         )

#         path = f"s3://{output_bucket}/{out_fp}/{linker._cache_uid}"

#         linker.profile_columns(
#             [
#                 "first_name",
#                 "surname",
#                 "first_name || surname",
#                 "concat(city, first_name)",
#                 ["surname", "city"],
#             ]
#         )

#         predict = linker.predict()

#         return linker, path, predict

#     linker, path, predict = run_athena_predictions()
#     linker.drop_all_tables_created_by_splink(tables_to_exclude=predict)

#     # Check everything gets cleaned up excl. predict
#     tables = wr.catalog.get_tables(
#         database=db_name_write,
#         name_prefix="__splink__df_predict",  # check if the predict table exists...
#         boto3_session=my_session,
#     )
#     assert sum(1 for _ in tables) == 1

#     # Check all files are also deleted (as delete_s3_folders = True)
#     files = wr.s3.list_objects(
#         path=path,
#         boto3_session=my_session,
#         ignore_empty=True,
#     )
#     assert len(files) > 0  # snappy, so n >= 1 parquet files created

#     folder_exists = wr.s3.list_directories(
#         path,
#     )
#     assert len(folder_exists) == 1  # check that only the predict table is saved

#     # Now drop *everything*
#     linker.drop_all_tables_created_by_splink()

#     # Does predict exist?
#     tables = wr.catalog.get_tables(
#         database=db_name_write,
#         name_prefix="__splink",
#         boto3_session=my_session,
#     )
#     assert sum(1 for _ in tables) == 0

#     # Check all files are also deleted (as gc = True)
#     files = wr.s3.list_objects(
#         path=path,
#         boto3_session=my_session,
#         ignore_empty=True,
#     )
#     assert len(files) == 0

#     # Check drop_tables_in_current_splink_run
#     linker, path, predict = run_athena_predictions()

#     linker.drop_tables_in_current_splink_run(tables_to_exclude=predict.physical_name)
#     assert len(linker._names_of_tables_created_by_splink) == 1
#     tables = wr.catalog.get_tables(
#         database=db_name_write,
#         name_prefix="__splink",
#         boto3_session=my_session,
#     )
#     assert sum(1 for _ in tables) == 1

#     linker.drop_tables_in_current_splink_run()
#     assert len(linker._names_of_tables_created_by_splink) == 0
#     tables = wr.catalog.get_tables(
#         database=db_name_write,
#         name_prefix="__splink",
#         boto3_session=my_session,
#     )
#     assert sum(1 for _ in tables) == 0


# def test_pandas_as_input():
#     df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
#     my_session = boto3.Session(region_name="eu-west-1")

#     linker = AthenaLinker(
#         input_table_or_tables=df,
#         settings_dict=settings_dict,
#         boto3_session=my_session,
#         output_bucket=output_bucket,
#         output_database=db_name_write,
#         output_filepath="test_pandas_input",
#         input_table_aliases="__splink__testing",
#     )

#     linker.predict()
#     linker.drop_splink_tables_from_database(database_name=db_name_read)


# def test_athena_link_only():
#     df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

#     # creates a session at least on the platform...
#     my_session = boto3.Session(region_name="eu-west-1")
#     settings_dict["link_type"] = "link_and_dedupe"

#     linker = AthenaLinker(
#         input_table_or_tables=[df, df],
#         settings_dict=settings_dict,
#         boto3_session=my_session,
#         output_bucket=output_bucket,
#         output_database=db_name_write,
#         output_filepath="test_link_only",
#         input_table_aliases=["__splink__testing_1", "__splink__testing_2"],
#     )

#     df_predict = linker.predict()
#     df_predict.as_pandas_dataframe()

#     # Clean up
#     linker.drop_all_tables_created_by_splink(delete_s3_folders=True)


# def test_athena_errors():
#     # creates a session at least on the platform...
#     my_session = boto3.Session(region_name="eu-west-1")

#     # Check that if the input df doesn't exist we get a fail
#     with pytest.raises(Exception):
#         AthenaLinker(
#             input_table_or_tables="testing_for_failure",
#             settings_dict=settings_dict,
#             boto3_session=my_session,
#             output_bucket=output_bucket,
#             output_database=db_name_write,
#             output_filepath="test_failure",
#         )

#     # Check that if the database doesn't exist it fails
#     rand_database = "random_database"
#     db_txt = f"database '{rand_database}'"

#     with pytest.raises(Exception) as excinfo:
#         AthenaLinker(
#             input_table_or_tables="testing_for_failure",
#             settings_dict=settings_dict,
#             boto3_session=my_session,
#             output_bucket=output_bucket,
#             output_database=rand_database,
#             output_filepath="test_failure",
#         )
#     assert str(excinfo.value) == athena_warning_text(db_txt, ["does", "it"])

#     # check we get a failure w/ an invalid s3 bucket
#     rand_bucket = "random_bucket"
#     bucket_txt = f"bucket '{rand_bucket}'"

#     with pytest.raises(Exception) as excinfo:
#         AthenaLinker(
#             input_table_or_tables="testing_for_failure",
#             settings_dict=settings_dict,
#             boto3_session=my_session,
#             output_bucket=rand_bucket,
#             output_database=db_name_write,
#             output_filepath="test_failure",
#         )
#     assert str(excinfo.value) == athena_warning_text(bucket_txt, ["does", "it"])

#     # and finally, check our error message w/ both an invalid db and bucket
#     txt = " and ".join([db_txt, bucket_txt])
#     with pytest.raises(Exception) as excinfo:
#         AthenaLinker(
#             input_table_or_tables="testing_for_failure",
#             settings_dict=settings_dict,
#             boto3_session=my_session,
#             output_bucket=rand_bucket,
#             output_database=rand_database,
#             output_filepath="test_failure",
#         )
#     assert str(excinfo.value) == athena_warning_text(txt, ["do", "them"])
