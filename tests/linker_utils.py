import os
import shutil

import pandas as pd
import pytest

from tests.cc_testing_utils import check_df_equality


def _test_table_registration(
    linker, additional_tables_to_register=[], skip_dtypes=False
):
    # Standard pandas df...
    a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    linker.register_table(a, "__splink_df_pd")
    pd_df = linker.query_sql("select * from __splink_df_pd", output_type="splinkdf")
    assert check_df_equality(pd_df.as_pandas_dataframe(), a, skip_dtypes)

    # Standard dictionary
    test_dict = {"a": [666, 777, 888], "b": [4, 5, 6]}
    t_dict = linker.register_table(test_dict, "__splink_df_test_dict")
    test_dict_df = pd.DataFrame(test_dict)
    assert check_df_equality(t_dict.as_pandas_dataframe(), test_dict_df, skip_dtypes)

    # Duplicate table name (check for error)
    with pytest.raises(ValueError):
        linker.register_table(test_dict, "__splink_df_pd")
    # Test overwriting works
    linker.register_table(test_dict_df, "__splink_df_pd", overwrite=True)
    out = linker.query_sql("select * from __splink_df_pd", output_type="pandas")
    assert check_df_equality(out, test_dict_df, skip_dtypes)

    # Record level dictionary
    b = [
        {"a": 1, "b": 22, "c": 333},
        {"a": 2, "b": 33, "c": 444},
        {"a": 3, "b": 44, "c": 555},
    ]

    linker.register_table(b, "__splink_df_record_df")
    record_df = linker.query_sql(
        "select * from __splink_df_record_df", output_type="pandas"
    )
    assert check_df_equality(record_df, pd.DataFrame.from_records(b), skip_dtypes)

    with pytest.raises(ValueError):
        linker.query_sql("select * from __splink_df_test_dict", output_type="testing")
    df = linker.query_sql(
        "select * from __splink_df_test_dict", output_type="splinkdf"
    ).as_pandas_dataframe()
    assert check_df_equality(df, test_dict_df, skip_dtypes)
    r_dict = linker.query_sql(
        "select * from __splink_df_record_df", output_type="splinkdf"
    ).as_record_dict()
    assert check_df_equality(
        pd.DataFrame.from_records(r_dict),
        pd.DataFrame.from_records(b),
        skip_dtypes,
    )

    # Test registration on additional data types for specific linkers
    if additional_tables_to_register:
        for table in additional_tables_to_register:
            linker.register_table(table, "test_table", overwrite=True)


def register_roc_data(linker):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df_10 = df.head(10).copy()
    df_10["merge"] = 1
    df_10["source_dataset"] = "fake_data_1"

    df_l = df_10[["unique_id", "source_dataset", "group", "merge"]].copy()
    df_r = df_l.copy()

    df_labels = df_l.merge(df_r, on="merge", suffixes=("_l", "_r"))
    f1 = df_labels["unique_id_l"] < df_labels["unique_id_r"]
    df_labels = df_labels[f1]

    df_labels["clerical_match_score"] = (
        df_labels["group_l"] == df_labels["group_r"]
    ).astype(float)

    df_labels = df_labels.drop(
        ["group_l", "group_r", "source_dataset_l", "source_dataset_r", "merge"],
        axis=1,
    )

    linker.register_table(df_labels, "labels")


def _test_write_functionality(linker, read_csv_func):
    root = "__splink_tests"
    # delete the folder and its contents
    if os.path.exists(root):
        shutil.rmtree(root)

    parquet_f = f"{root}/tmp_files/test.parquet"
    linker.predict().to_parquet(parquet_f)
    assert len(pd.read_parquet(parquet_f)) == 3167
    # Duplicate table name (check for error)
    with pytest.raises(FileExistsError):
        linker.predict().to_parquet(parquet_f)

    csv_f = f"{root}/tmp_files/test.csv"
    linker.predict().to_csv(csv_f)
    assert len(read_csv_func(csv_f)) == 3167
    # Duplicate table name (check for error)
    with pytest.raises(FileExistsError):
        linker.predict().to_csv(csv_f)

    # delete the folder and its contents
    shutil.rmtree(root)
