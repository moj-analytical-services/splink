from copy import deepcopy

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

dummy_demo_data = {"first_name": "John", "surname": "Smith", "dob": "01/01/1980"}
data = [
    {"id": 1, "source_ds": "d"},
    {"id": 2, "source_ds": "d"},
    {"id": 1, "source_ds": "b"},
    {"id": 2, "source_ds": "b"},
    {"id": 3, "source_ds": "c"},
    {"id": 4, "source_ds": "c"},
]


def dropkey(data_dict, key):
    del data_dict[key]
    return data_dict


sds_b_only = [
    dropkey({**row, **dummy_demo_data}, "source_ds")
    for row in data
    if row["source_ds"] == "b"
]
sds_c_only = [
    dropkey({**row, **dummy_demo_data}, "source_ds")
    for row in data
    if row["source_ds"] == "c"
]
sds_d_only = [
    dropkey({**row, **dummy_demo_data}, "source_ds")
    for row in data
    if row["source_ds"] == "d"
]


def test_dedupe_only_join_condition():
    data = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
        {"id": 5},
        {"id": 6},
    ]

    settings = deepcopy(settings_template)
    settings["link_type"] = "dedupe_only"

    db_api = DuckDBAPI()
    df_sdf = db_api.register([{**row, **dummy_demo_data} for row in data])
    linker = Linker(df_sdf, settings)
    predict_rows = linker.inference.predict().as_record_dict()

    assert len(predict_rows) == (6 * 5) / 2

    # Check that the lower ID is always on the left hand side
    assert all(row["id_l"] < row["id_r"] for row in predict_rows)


def test_link_only_two_join_condition():
    settings = deepcopy(settings_template)
    settings["link_type"] = "link_only"

    db_api = DuckDBAPI()
    sds_d_sdf = db_api.register(sds_d_only)
    sds_b_sdf = db_api.register(sds_b_only)
    linker = Linker([sds_d_sdf, sds_b_sdf], settings)
    predict_rows = linker.inference.predict().as_record_dict()

    assert len(predict_rows) == 4

    # Check that the lower ID is always on the left hand side
    predict_rows = [
        {
            **row,
            "id_concat_l": f"{row['source_dataset_l']}-__-{row['id_l']}",
            "id_concat_r": f"{row['source_dataset_r']}-__-{row['id_r']}",
        }
        for row in predict_rows
    ]
    assert all(row["id_concat_l"] < row["id_concat_r"] for row in predict_rows)


def test_link_only_three_join_condition():
    settings = deepcopy(settings_template)
    settings["link_type"] = "link_only"

    db_api = DuckDBAPI()
    sds_d_sdf = db_api.register(sds_d_only)
    sds_b_sdf = db_api.register(sds_b_only)
    sds_c_sdf = db_api.register(sds_c_only)
    linker = Linker([sds_d_sdf, sds_b_sdf, sds_c_sdf], settings)
    predict_rows = linker.inference.predict().as_record_dict()

    assert len(predict_rows) == 12

    # Check that the lower ID is always on the left hand side
    predict_rows = [
        {
            **row,
            "id_concat_l": f"{row['source_dataset_l']}-__-{row['id_l']}",
            "id_concat_r": f"{row['source_dataset_r']}-__-{row['id_r']}",
        }
        for row in predict_rows
    ]
    assert all(row["id_concat_l"] < row["id_concat_r"] for row in predict_rows)


def test_link_and_dedupe_two_join_condition():
    settings = deepcopy(settings_template)
    settings["link_type"] = "link_and_dedupe"

    db_api = DuckDBAPI()
    sds_d_sdf = db_api.register(sds_d_only)
    sds_b_sdf = db_api.register(sds_b_only)
    linker = Linker([sds_d_sdf, sds_b_sdf], settings)
    predict_rows = linker.inference.predict().as_record_dict()

    assert len(predict_rows) == (4 * 3) / 2

    # Check that the lower ID is always on the left hand side
    predict_rows = [
        {
            **row,
            "id_concat_l": f"{row['source_dataset_l']}-__-{row['id_l']}",
            "id_concat_r": f"{row['source_dataset_r']}-__-{row['id_r']}",
        }
        for row in predict_rows
    ]
    assert all(row["id_concat_l"] < row["id_concat_r"] for row in predict_rows)


def test_link_and_dedupe_three_join_condition():
    settings = deepcopy(settings_template)
    settings["link_type"] = "link_and_dedupe"

    db_api = DuckDBAPI()
    sds_d_sdf = db_api.register(sds_d_only)
    sds_b_sdf = db_api.register(sds_b_only)
    sds_c_sdf = db_api.register(sds_c_only)
    linker = Linker([sds_d_sdf, sds_b_sdf, sds_c_sdf], settings)
    predict_rows = linker.inference.predict().as_record_dict()

    assert len(predict_rows) == (6 * 5) / 2

    # Check that the lower ID is always on the left hand side
    predict_rows = [
        {
            **row,
            "id_concat_l": f"{row['source_dataset_l']}-__-{row['id_l']}",
            "id_concat_r": f"{row['source_dataset_r']}-__-{row['id_r']}",
        }
        for row in predict_rows
    ]
    assert all(row["id_concat_l"] < row["id_concat_r"] for row in predict_rows)
