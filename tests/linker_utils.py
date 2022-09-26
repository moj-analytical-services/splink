import pandas as pd
from tests.cc_testing_utils import check_df_equality
import pytest
import pyarrow as pa


def _test_table_registration(linker):

    # Standard pandas df...
    a = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    linker.register_table(a, "pd")
    pd_df = linker.query_sql("select * from pd")
    assert check_df_equality(pd_df, a)

    # Standard dictionary
    test_dict = {"a": [666, 777, 888], "b": [4, 5, 6]}
    linker.register_table(test_dict, "test_dict")
    t_dict = linker.query_sql("select * from test_dict")
    assert check_df_equality(t_dict, pd.DataFrame(test_dict))

    # Duplicate table name (check for error)
    with pytest.raises(ValueError):
        linker.register_table(test_dict, "pd")
    # Test overwriting works
    try:
        linker.register_table(test_dict, "pd", overwrite=True)
    except:
        assert False

    # Record level dictionary
    b = [
        {"a": 1, "b": 22, "c": 333},
        {"a": 2, "b": 33, "c": 444},
        {"a": 3, "b": 44, "c": 555},
    ]

    linker.register_table(b, "record_df")
    record_df = linker.query_sql("select * from record_df")
    assert check_df_equality(record_df, pd.DataFrame.from_records(b))


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
