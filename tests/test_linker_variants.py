import pandas as pd
import pytest
from splink.comparison_library import exact_match


def dfs():

    data = [
        {"id_link": 1, "id_dedupe": 1, "source_ds": "a"},
        {"id_link": 2, "id_dedupe": 2, "source_ds": "a"},
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

    sds_a_only = (
        df.query("source_ds == 'a'")
        .rename(columns={"id_link": "id"})
        .drop(columns=["id_dedupe"], axis=1)
    )

    sds_b_only = (
        df.query("source_ds == 'b'")
        .rename(columns={"id_link": "id"})
        .drop(columns=["id_dedupe"], axis=1)
    )

    sds_c_only = (
        df.query("source_ds == 'c'")
        .rename(columns={"id_link": "id"})
        .drop(columns=["id_dedupe"], axis=1)
    )

    return {
        "dedupe_only__pass": {"df": dedupe_only},
        "link_only__two": {"df_a": sds_a_only.copy(), "df_b": sds_b_only.copy()},
        "link_only__three": {
            "df_a": sds_a_only.copy(),
            "df_b": sds_b_only.copy(),
            "df_c": sds_c_only.copy(),
        },
        "link_and_dedupe__two": {"df_a": sds_a_only.copy(), "df_b": sds_b_only.copy()},
        "link_and_dedupe__three": {
            "df_a": sds_a_only.copy(),
            "df_b": sds_b_only.copy(),
            "df_c": sds_c_only.copy(),
        },
    }


@pytest.mark.parametrize("input_name,input_tables", dfs().items())
def test_link_type(input_name, input_tables):
    link_type, test_name = input_name.split("__")

    input_tables = dfs()[input_name]
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

    linker = DuckDBLinker(settings, input_tables=input_tables, connection=":memory:")

    linker.train_u_using_random_sampling(target_rows=1e6)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    linker.predict()

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
