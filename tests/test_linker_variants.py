import pandas as pd
import pytest
from splink.comparison_library import exact_match


def dfs():

    data = [
        {"id_link": 1, "id_dedupe": 1, "source_ds": "d"},
        {"id_link": 2, "id_dedupe": 2, "source_ds": "d"},
        {"id_link": 1, "id_dedupe": 3, "source_ds": "b"},
        {"id_link": 2, "id_dedupe": 4, "source_ds": "b"},
        {"id_link": 3, "id_dedupe": 5, "source_ds": "c"},
        {"id_link": 4, "id_dedupe": 6, "source_ds": "c"},
    ]

    df = pd.DataFrame(data)
    df["first_name"] = "John"
    df["surname"] = "Smith"
    df["dob"] = "01/01/1980"

    dedupe_only = df.rename(columns={"id_dedupe": "id"}).drop(
        columns=[
            "source_ds",
            "id_link",
        ],
        axis=1,
    )

    sds_d_only = (
        df.query("source_ds == 'd'")
        .rename(columns={"id_link": "id"})
        .drop(columns=["id_dedupe", "source_ds"], axis=1)
    )

    sds_b_only = (
        df.query("source_ds == 'b'")
        .rename(columns={"id_link": "id"})
        .drop(columns=["id_dedupe", "source_ds"], axis=1)
    )

    sds_c_only = (
        df.query("source_ds == 'c'")
        .rename(columns={"id_link": "id"})
        .drop(columns=["id_dedupe", "source_ds"], axis=1)
    )

    return {
        "dedupe_only__pass": {"input_tables": dedupe_only, "input_table_aliases": "df"},
        "link_only__two": {
            "input_tables": [sds_d_only, sds_b_only],
            "input_table_aliases": [
                "df_d",
                "df_b",
            ],
        },
        "link_only__three": {
            "input_tables": [sds_d_only, sds_b_only, sds_c_only],
            "input_table_aliases": ["df_d", "df_b", "df_c"],
        },
        "link_and_dedupe__two": {
            "input_tables": [sds_d_only, sds_b_only],
            "input_table_aliases": [
                "df_d",
                "df_b",
            ],
        },
        "link_and_dedupe__three": {
            "input_tables": [sds_d_only, sds_b_only, sds_c_only],
            "input_table_aliases": ["df_d", "df_b", "df_c"],
        },
    }


@pytest.mark.parametrize("input_name,input_tables", dfs().items())
def test_link_type(input_name, input_tables):
    # Basic check that no errors are generated for any of the link types
    link_type, test_name = input_name.split("__")

    input_tables = dfs()[input_name]["input_tables"]
    aliases = dfs()[input_name]["input_table_aliases"]
    settings = {
        "proportion_of_matches": 0.01,
        "unique_id_column_name": "id",
        "link_type": link_type,
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [
            exact_match("first_name"),
            exact_match("surname"),
            exact_match("dob"),
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
        "max_iterations": 2,
    }

    from splink.duckdb.duckdb_linker import DuckDBLinker

    linker = DuckDBLinker(
        input_tables, settings, connection=":memory:", input_table_aliases=aliases
    )

    linker.estimate_u_using_random_sampling(target_rows=1e6)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_e = linker.predict()

    df_len = len(df_e.as_pandas_dataframe())
    if input_name == "dedupe_only__pass":
        assert df_len == (6 * 5) / 2
    if input_name == "link_only__two":
        assert df_len == 4
    if input_name == "link_only__three":
        assert df_len == 12
    if input_name == "link_and_dedupe__two":
        assert df_len == (4 * 3) / 2
    if input_name == "link_and_dedupe__three":
        assert df_len == (6 * 5) / 2

    # Check that the lower ID is always on the left hand side
    df_e_pd = df_e.as_pandas_dataframe()
    if link_type == "dedupe_only":
        df_e_pd["id_concat_l"] = df_e_pd["id_l"]
        df_e_pd["id_concat_r"] = df_e_pd["id_r"]
    else:
        df_e_pd["id_l"] = df_e_pd["id_l"].astype(str)
        df_e_pd["id_r"] = df_e_pd["id_r"].astype(str)
        df_e_pd["id_concat_l"] = df_e_pd["source_dataset_l"] + "-__-" + df_e_pd["id_l"]
        df_e_pd["id_concat_r"] = df_e_pd["source_dataset_r"] + "-__-" + df_e_pd["id_r"]

    assert all(df_e_pd["id_concat_l"] < df_e_pd["id_concat_r"])

    record_1 = {
        "id": 1,
        "first_name": "George",
        "surname": "Smith",
        "dob": "1957-02-17",
    }

    record_2 = {
        "id": 2,
        "first_name": "George",
        "surname": "Smith",
        "dob": "1957-02-17",
    }

    linker.compare_two_records(record_1, record_2)

    linker.find_matches_to_new_records(
        [record_1], blocking_rules=[]
    ).as_pandas_dataframe()
