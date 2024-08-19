from copy import deepcopy

import pandas as pd

from splink.internals.comparison_library import ExactMatch
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker

settings_template = {
    "probability_two_random_records_match": 0.01,
    "unique_id_column_name": "id",
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
    "comparisons": [
        ExactMatch("first_name").configure(term_frequency_adjustments=True),
        ExactMatch("surname"),
        ExactMatch("dob"),
    ],
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
}

data = [
    {"id": 1, "source_ds": "d"},
    {"id": 2, "source_ds": "d"},
    {"id": 1, "source_ds": "b"},
    {"id": 2, "source_ds": "b"},
    {"id": 3, "source_ds": "c"},
    {"id": 4, "source_ds": "c"},
]


df = pd.DataFrame(data)
df["first_name"] = "John"
df["surname"] = "Smith"
df["dob"] = "01/01/1980"

sds_b_only = df.query("source_ds == 'b'").drop(columns=["source_ds"], axis=1)

sds_c_only = df.query("source_ds == 'c'").drop(columns=["source_ds"], axis=1)

sds_d_only = df.query("source_ds == 'd'").drop(columns=["source_ds"], axis=1)


def test_dedupe_only_join_condition():
    data = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
        {"id": 5},
        {"id": 6},
    ]

    df = pd.DataFrame(data)
    df["first_name"] = "John"
    df["surname"] = "Smith"
    df["dob"] = "01/01/1980"

    settings = deepcopy(settings_template)
    settings["link_type"] = "dedupe_only"

    settings_salt = deepcopy(settings_template)
    settings_salt["link_type"] = "dedupe_only"

    for s in [settings, settings_salt]:
        db_api = DuckDBAPI()

        linker = Linker(df.copy(), s, db_api=db_api)

        df_predict = linker.inference.predict().as_pandas_dataframe()

        assert len(df_predict) == (6 * 5) / 2

        # Check that the lower ID is always on the left hand side
        assert all(df_predict["id_l"] < df_predict["id_r"])

        # self_link = linker._self_link().as_pandas_dataframe()
        # assert len(self_link) == len(data)


def test_link_only_two_join_condition():
    settings = deepcopy(settings_template)

    settings = deepcopy(settings_template)
    settings["link_type"] = "link_only"

    settings_salt = deepcopy(settings_template)
    settings_salt["link_type"] = "link_only"

    for s in [settings, settings_salt]:
        db_api = DuckDBAPI()

        linker = Linker([sds_d_only, sds_b_only], s, db_api=db_api)

        df_predict = linker.inference.predict().as_pandas_dataframe()

        assert len(df_predict) == 4

        # Check that the lower ID is always on the left hand side
        df_predict["id_concat_l"] = (
            df_predict["source_dataset_l"] + "-__-" + df_predict["id_l"].astype(str)
        )
        df_predict["id_concat_r"] = (
            df_predict["source_dataset_r"] + "-__-" + df_predict["id_r"].astype(str)
        )
        assert all(df_predict["id_concat_l"] < df_predict["id_concat_r"])

        # self_link = linker._self_link().as_pandas_dataframe()
        # assert len(self_link) == 4


def test_link_only_three_join_condition():
    settings = deepcopy(settings_template)
    settings["link_type"] = "link_only"

    settings_salt = deepcopy(settings_template)
    settings_salt["link_type"] = "link_only"

    for s in [settings, settings_salt]:
        db_api = DuckDBAPI()

        linker = Linker([sds_d_only, sds_b_only, sds_c_only], s, db_api=db_api)

        df_predict = linker.inference.predict().as_pandas_dataframe()

        assert len(df_predict) == 12

        # Check that the lower ID is always on the left hand side
        df_predict["id_concat_l"] = (
            df_predict["source_dataset_l"] + "-__-" + df_predict["id_l"].astype(str)
        )
        df_predict["id_concat_r"] = (
            df_predict["source_dataset_r"] + "-__-" + df_predict["id_r"].astype(str)
        )
        assert all(df_predict["id_concat_l"] < df_predict["id_concat_r"])

        # self_link = linker._self_link().as_pandas_dataframe()
        # assert len(self_link) == 6


def test_link_and_dedupe_two_join_condition():
    settings = deepcopy(settings_template)
    settings["link_type"] = "link_and_dedupe"

    settings_salt = deepcopy(settings_template)
    settings_salt["link_type"] = "link_and_dedupe"

    for s in [settings, settings_salt]:
        db_api = DuckDBAPI()

        linker = Linker([sds_d_only, sds_b_only], s, db_api=db_api)

        df_predict = linker.inference.predict().as_pandas_dataframe()

        assert len(df_predict) == (4 * 3) / 2

        # Check that the lower ID is always on the left hand side
        df_predict["id_concat_l"] = (
            df_predict["source_dataset_l"] + "-__-" + df_predict["id_l"].astype(str)
        )
        df_predict["id_concat_r"] = (
            df_predict["source_dataset_r"] + "-__-" + df_predict["id_r"].astype(str)
        )
        assert all(df_predict["id_concat_l"] < df_predict["id_concat_r"])

        # self_link = linker._self_link().as_pandas_dataframe()
        # assert len(self_link) == 4


def test_link_and_dedupe_three_join_condition():
    settings = deepcopy(settings_template)
    settings["link_type"] = "link_and_dedupe"

    settings_salt = deepcopy(settings_template)
    settings_salt["link_type"] = "link_and_dedupe"

    for s in [settings, settings_salt]:
        db_api = DuckDBAPI()

        linker = Linker([sds_d_only, sds_b_only, sds_c_only], s, db_api=db_api)

        df_predict = linker.inference.predict().as_pandas_dataframe()

        assert len(df_predict) == (6 * 5) / 2

        # Check that the lower ID is always on the left hand side
        df_predict["id_concat_l"] = (
            df_predict["source_dataset_l"] + "-__-" + df_predict["id_l"].astype(str)
        )
        df_predict["id_concat_r"] = (
            df_predict["source_dataset_r"] + "-__-" + df_predict["id_r"].astype(str)
        )
        assert all(df_predict["id_concat_l"] < df_predict["id_concat_r"])

        # self_link = linker._self_link().as_pandas_dataframe()
        # assert len(self_link) == 4
