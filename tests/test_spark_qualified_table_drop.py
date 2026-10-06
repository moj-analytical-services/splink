import pytest

pytest.importorskip("pyspark")
# ruff: noqa: E402

from splink.internals.spark.database_api import SparkAPI

from .decorator import mark_with_dialects_including


@mark_with_dialects_including("spark")
def test_qualified_drop_removes_table_from_non_current_database(spark):
    spark.catalog.setCurrentDatabase("default")
    spark.sql("CREATE DATABASE IF NOT EXISTS splink_drop_target")
    name = "__splink__df_neighbours_qualified_drop"
    try:
        api = SparkAPI(
            spark_session=spark,
            database="splink_drop_target",
            break_lineage_method="delta_lake_table",
        )
        spark.createDataFrame([{"id": 1}]).write.mode("overwrite").saveAsTable(
            f"{api.splink_data_store}.{name}"
        )
        assert spark.catalog.tableExists(f"splink_drop_target.{name}")

        splink_df = api.table_to_splink_dataframe("__splink__df_neighbours", name)
        splink_df.created_by_splink = True
        splink_df.drop_table_from_database_and_remove_from_cache()

        assert not spark.catalog.tableExists(f"splink_drop_target.{name}")
    finally:
        spark.catalog.setCurrentDatabase("default")
        spark.sql("DROP DATABASE IF EXISTS splink_drop_target CASCADE")
