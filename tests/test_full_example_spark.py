from splink.spark.spark_linker import SparkLinker


from basic_settings import settings_dict


def test_full_example_spark(df_spark):

    linker = SparkLinker(settings_dict, input_tables={"fake_data_1": df_spark})

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
    )
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.train_u_using_random_sampling(target_rows=1e6)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    spark = df_spark.sql_ctx.sparkSession

    spark.catalog.cacheTable(df_predict.physical_name)

    linker.splink_comparison_viewer(df_predict, "test_scv2.html", True, 2)
