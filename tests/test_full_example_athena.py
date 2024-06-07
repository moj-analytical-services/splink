# import os

# import pandas as pd
# import pytest

# import splink.athena.comparison_library as cl
# from splink.exceptions import InvalidAWSBucketOrDatabase

# from .basic_settings import get_settings_dict
# from .linker_utils import _test_table_registration

# # Skip tests if awswrangler or boto3 cannot be imported or
# # if no valid AWS connection exists
# try:
#     import awswrangler as wr
#     import boto3
#     from awswrangler.exceptions import InvalidTable

#     from splink.athena.athena_helpers.athena_utils import _garbage_collection
#     from splink.athena.linker import AthenaLinker

#     sts_client = boto3.client("sts")
#     response = sts_client.get_caller_identity()
#     aws_connection_valid = response
#     BOTO3_SESSION = boto3.Session(region_name="eu-west-1")
#     aws_dependencies_available = True
# except ImportError:
#     # If InvalidTable cannot be imported, we need to create a temp value
#     # to prevent an ImportError
#     class InvalidTable(Exception):
#         ...

#     # An import error is equivalent to a missing AWS connection
#     aws_connection_valid = False


# # Conditional skipping of tests if AWS dependencies are not satisfied
# pytestmark = pytest.mark.skipif(
#     not aws_connection_valid, reason="AWS Connection and Dependencies Required"
# )

# # Continue with th# Load in and update our settings
# settings_dict = get_settings_dict()

# first_name_cc = cl.LevenshteinAtThresholds(
#     col_name="first_name",
#     distance_threshold_or_thresholds=2,
#     include_exact_match_level=True,
#     term_frequency_adjustments=True,
#     m_probability_exact_match=0.7,
#     m_probability_or_probabilities_lev=0.2,
#     m_probability_else=0.1,
# )

# dob_cc = cl.datediff_at_thresholds(
#     col_name="dob",
#     date_thresholds=[7, 3, 1],
#     date_metrics=["day", "month", "year"],
#     cast_strings_to_date=True,
# )

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
# settings_dict["comparisons"][2] = dob_cc

# # Setup database names for tests
# DB_NAME_READ = "splink_awswrangler_test"
# DB_NAME_WRITE = "data_linking_temp"
# OUTPUT_BUCKET = "alpha-data-linking"
# TABLE_NAME = "__splink__fake_1000_from_splink_demos"
# PANDAS_DF = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")


# def upload_data(database_to_upload_to):
#     # ensure data is in a unique loc to __splink tables
#     path = f"s3://{OUTPUT_BUCKET}/data/{TABLE_NAME}"
#     wr.s3.to_parquet(
#         df=PANDAS_DF,
#         path=path,
#         dataset=True,
#         mode="overwrite",
#         database=database_to_upload_to,
#         table=TABLE_NAME,
#         compression="snappy",
#     )


# def test_full_example_athena(tmp_path):
#     # Upload our raw data
#     upload_data(DB_NAME_READ)

#     # Drop any existing tables that may disrupt the test
#     _garbage_collection(DB_NAME_WRITE, BOTO3_SESSION)

#     linker = AthenaLinker(
#         settings_dict=settings_dict,
#         input_table_or_tables=f"{DB_NAME_READ}.{TABLE_NAME}",
#         boto3_session=BOTO3_SESSION,
#         output_bucket=OUTPUT_BUCKET,
#         output_database=DB_NAME_WRITE,
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
#     linker.table_management.compute_tf_table("city")
#     linker.table_management.compute_tf_table("first_name")

#     linker.training.estimate_u_using_random_sampling(max_pairs=1e6, seed=None)

#     blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
#     linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

#     blocking_rule = "l.dob = r.dob"
#     linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

#     df_predict = linker.inference.predict()

#     linker.visualisations.comparison_viewer_dashboard(
#           df_predict, "test_scv_athena.html", True, 2
#     )

#     df_predict.as_pandas_dataframe()

#     df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
#           df_predict, 0.1)

#     linker.visualisations.cluster_studio_dashboard(
#         df_predict,
#         df_clusters,
#         sampling_method="by_cluster_size",
#         out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
#     )

#     linker.evaluation.unlinkables_chart(source_dataset="Testing")

#     _test_table_registration(linker)

#     # Clean up our database and s3 bucket and remove test files
#     linker.drop_all_tables_created_by_splink(delete_s3_folders=True)
#     linker.drop_splink_tables_from_database(database_name=DB_NAME_READ)


# def test_athena_garbage_collection():
#     # creates a session at least on the platform...
#     upload_data(DB_NAME_READ)

#     out_fp = "athena_test_garbage_collection"

#     def run_athena_predictions():
#         # Test that our gc works as expected w/ tables_to_exclude
#         linker = AthenaLinker(
#             settings_dict=settings_dict,
#             input_table_or_tables=f"{DB_NAME_READ}.{TABLE_NAME}",
#             boto3_session=BOTO3_SESSION,
#             output_bucket=OUTPUT_BUCKET,
#             output_database=DB_NAME_WRITE,
#             output_filepath=out_fp,
#         )

#         path = f"s3://{OUTPUT_BUCKET}/{out_fp}/{linker._cache_uid}"

#         linker.profile_columns(
#             [
#                 "first_name",
#                 "surname",
#                 "first_name || surname",
#                 "concat(city, first_name)",
#                 ["surname", "city"],
#             ]
#         )

#         predict = linker.inference.predict()

#         return linker, path, predict

#     linker, path, predict = run_athena_predictions()
#     linker.drop_all_tables_created_by_splink(tables_to_exclude=predict)

#     # Check everything gets cleaned up excl. predict
#     tables = wr.catalog.get_tables(
#         database=DB_NAME_WRITE,
#         name_prefix="__splink__df_predict",  # check if the predict table exists...
#         boto3_session=BOTO3_SESSION,
#     )
#     assert sum(1 for _ in tables) == 1

#     # Check all files are also deleted (as delete_s3_folders = True)
#     files = wr.s3.list_objects(
#         path=path,
#         boto3_session=BOTO3_SESSION,
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
#         database=DB_NAME_WRITE,
#         name_prefix="__splink",
#         boto3_session=BOTO3_SESSION,
#     )
#     assert sum(1 for _ in tables) == 0

#     # Check all files are also deleted (as gc = True)
#     files = wr.s3.list_objects(
#         path=path,
#         boto3_session=BOTO3_SESSION,
#         ignore_empty=True,
#     )
#     assert len(files) == 0

#     # Check drop_tables_in_current_splink_run
#     linker, path, predict = run_athena_predictions()

#     linker.drop_tables_in_current_splink_run(tables_to_exclude=predict.physical_name)
#     # assert len(linker._names_of_tables_created_by_splink) == 1
#     tables = wr.catalog.get_tables(
#         database=DB_NAME_WRITE,
#         name_prefix="__splink",
#         boto3_session=BOTO3_SESSION,
#     )
#     assert sum(1 for _ in tables) == 1

#     linker.drop_tables_in_current_splink_run()
#     # assert len(linker._names_of_tables_created_by_splink) == 0
#     tables = wr.catalog.get_tables(
#         database=DB_NAME_WRITE,
#         name_prefix="__splink",
#         boto3_session=BOTO3_SESSION,
#     )
#     assert sum(1 for _ in tables) == 0


# @pytest.mark.parametrize(
#     "input_tables, table_aliases, link_type",
#     [
#         (PANDAS_DF, "__splink__testing", "dedupe_only"),
#         (
#             [PANDAS_DF, PANDAS_DF],
#             ["__splink__testing_1", "__splink__testing_2"],
#             "link_and_dedupe",
#         ),
#     ],
# )
# def test_athena_linker_with_pandas(input_tables, table_aliases, link_type):
#     settings = get_settings_dict()
#     settings["link_type"] = link_type  # If this setting is common to both tests

#     linker = AthenaLinker(
#         input_table_or_tables=input_tables,
#         settings_dict=settings,
#         boto3_session=BOTO3_SESSION,
#         output_bucket=OUTPUT_BUCKET,
#         output_database=DB_NAME_WRITE,
#         output_filepath="test_pandas_as_inputs",
#         input_table_aliases=table_aliases,
#     )

#     df_predict = linker.inference.predict()
#     df_predict.as_pandas_dataframe()

#     linker.drop_all_tables_created_by_splink(delete_s3_folders=True)


# @pytest.mark.parametrize(
#     "input_table, output_database, output_bucket, exception",
#     [
#         ("bad_df", DB_NAME_WRITE, OUTPUT_BUCKET, InvalidTable),
#         (PANDAS_DF, "random_database", OUTPUT_BUCKET, InvalidAWSBucketOrDatabase),
#         (PANDAS_DF, DB_NAME_WRITE, "random_bucket", InvalidAWSBucketOrDatabase),
#         (PANDAS_DF, "random_database", "random_bucket", InvalidAWSBucketOrDatabase),
#     ],
# )
# def test_athena_errors(input_table, output_database, output_bucket, exception):
#     test_file_path = "test_failure"

#     with pytest.raises(exception):
#         AthenaLinker(
#             input_table_or_tables=input_table,
#             settings_dict=settings_dict,
#             boto3_session=BOTO3_SESSION,
#             output_bucket=output_bucket,
#             output_database=output_database,
#             output_filepath=test_file_path,
#         )
