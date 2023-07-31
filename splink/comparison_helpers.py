import duckdb
import pandas as pd

# import phonetics

from splink.duckdb.linker import DuckDBLinker

from .blocking import block_using_rules_sql
from .charts import (
    _comparator_score_chart,
    _comparator_score_threshold_chart,
    _phonetic_match_chart,
)
from .comparison_helpers_utils import threshold_match
from .comparison_vector_values import compute_comparison_vector_values_sql
from .settings import Settings


def get_comparison_levels(
    values_to_compare: list | dataframe, comparison, max_df_rows=5
):
    """
    Helper function returning the comparison levels that all combinations of
    values in the values_to_compare list.

    Notes:

    * This function is intended for development purposes so uses the
    DuckDBLinker in the background. Any comparison levels should translate across
    to any Splink backend (assuming all of the comparison levels are available).
    * This function currently only works with comparisons based on a single
    column. For example, `cl.distance_in_km_at_thresholds` is not currently supported
    at it requires a `"lat_col"` and `"long_col"`.

    Args:
        values_to_compare (list): A list of values to compare in the
            comparison.
        comparison (Comparison): A DuckDB Comparison object defining the comparison
            levels to assign the pairs of values in values_to_compare.
        max_df_rows (int): When values_to_compare, the maximum number of rows to
            compare. Defaults to 5.

    Examples:
        Check comparison levels for a list of names
        ```py
        import splink.comparison_helpers as ch
        import splink.duckdb.comparison_template_library as ctl

        values = ["Robert", "Rob", "Robbie", "Robin"]
        comparison = ctl.name_comparison("name")

        ch.get_comparison_levels(values, comparison)
        ```
        Check comparison_levels for a list of dates
        ```py
        import splink.comparison_helpers as ch
        import splink.duckdb.comparison_template_library as ctl

        values = ["2022-01-01", "2022-02-01"]
        comparison = ctl.date_comparison("dob", cast_strings_to_date=True)

        ch.get_comparison_levels(values, comparison)
        ```


    """
    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [
            comparison,
        ],
    }
    settings_obj = Settings(settings)

    comparison_dict = settings_obj.comparisons[0].as_dict()

    if isinstance(values_to_compare, pd.Dataframe):
        values_df = values_to_compare
    elif isinstance(values_to_compare, list):
        comparison_col = comparison_dict["output_column_name"]
        values_df = pd.DataFrame(values_to_compare, columns=[comparison_col])

    values_df["unique_id"] = values_df.reset_index().index

    linker = DuckDBLinker(values_df, settings)

    input_dataframes = [linker._initialise_df_concat_with_tf()]

    sql = block_using_rules_sql(linker)
    linker._enqueue_sql(sql, "__splink__df_blocked")
    sql = compute_comparison_vector_values_sql(linker._settings_obj)
    linker._enqueue_sql(sql, "__splink__df_comparison_vectors")
    df = linker._execute_sql_pipeline(input_dataframes).as_pandas_dataframe()
    df[comparison_col] = df.apply(
        lambda row: [row[f"{comparison_col}_l"], row[f"{comparison_col}_r"]], axis=1
    )

    labels = pd.DataFrame(comparison_dict["comparison_levels"])
    labels["gamma"] = labels.index[::-1]
    labels.loc[labels["is_null_level"] == True, "gamma"] = -1

    comp_df = pd.merge(
        df,
        labels[["label_for_charts", "gamma"]],
        left_on=f"gamma_{comparison_col}",
        right_on="gamma",
        how="right",
    )
    comp_df = comp_df[["label_for_charts", "gamma", f"{comparison_col}"]].rename(
        columns={"label_for_charts": "comparison_level"}
    )
    comp_df.fillna("", inplace=True)
    comp_df = comp_df.groupby(["comparison_level", "gamma"]).agg(
        {comparison_col: lambda x: list(x)}
    )
    comp_df = comp_df.sort_values(by="gamma", ascending=False)

    return comp_df


comparator_cols_sql = """
    levenshtein({comparison1}, {comparison2}) as levenshtein_distance,
    damerau_levenshtein({comparison1}, {comparison2}) as damerau_levenshtein_distance,
    ROUND(jaro_similarity(
        {comparison1}, {comparison2}), {decimal_places}
        ) as jaro_similarity,
    ROUND(jaro_winkler_similarity(
        {comparison1}, {comparison2}), {decimal_places}
        ) as jaro_winkler_similarity,
    ROUND(jaccard({comparison1}, {comparison2}), {decimal_places}) as jaccard_similarity
"""


def comparator_score(str1, str2, decimal_places=2):
    """
    Helper function to give the similarity between two strings for
    the string comparators in splink.

    Args:
        str1 (str): String to compare to str2.
        str2 (str): String to compare to str1.
        decimal_places (int, optional): Number of decimal places to return with comparator
             scores.

    Examples:
        ```py
        import splink.comparison_helpers as ch

        ch.comparator_score("Richard", "iRchard")
        ```
    """
    con = duckdb.connect()

    sql = f"""
        select
        '{str1}' as string1,
        '{str2}' as string2,
        {comparator_cols_sql.format(
            comparison1 = 'string1',
            comparison2 = 'string2',
            decimal_places=decimal_places
        )}
    """
    return con.execute(sql).fetch_df()


def comparator_score_df(dict, col1, col2, decimal_places=2):
    """
    Helper function returning a dataframe showing the string similarity
    scores and string distances for a dictionary of strings.

    Args:
        dict (dictionary): Dictionary of string pairs to compare
        col1 (str):  The name of the first key in dictionary
        col2 (str):  The name of the second key in dictionary
        decimal_places (int, optional): Number of decimal places to return with comparator
             scores.

    Examples:
        ```py
        import splink.comparison_helpers as ch

        data = {
                "string1": ["Stephen", "Stephen","Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.comparator_score_df(data, "string1", "string2")
        ```
    """
    duckdb.connect()

    pd.DataFrame(list)

    sql = f"""
        select
        {col1}, {col2},
        {comparator_cols_sql.format(
            comparison1 = col1,
            comparison2 = col2,
            decimal_places=decimal_places
        )},
        from df
    """

    return duckdb.sql(sql).df()


def comparator_score_chart(dict, col1, col2):
    """
    Helper function returning a heatmap showing the sting similarity
    scores and string distances for a dictionary of strings.

    Args:
        dict (dictionary): Dictionary of string pairs to compare
        col1 (str):  The name of the first key in dictionary
        col2 (str):  The name of the second key in dictionary

    Examples:
        ```py
        import splink.comparison_helpers as ch

        data = {
                "string1": ["Stephen", "Stephen", "Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.comparator_score_chart(data, "string1", "string2")
        ```
    """

    df = comparator_score_df(list, col1, col2)

    df["strings_to_compare"] = df["string1"] + ", " + df["string2"]

    df_long = pd.melt(
        df,
        id_vars=["strings_to_compare"],
        value_vars=[
            "jaro_similarity",
            "jaro_winkler_similarity",
            "jaccard_similarity",
            "levenshtein_distance",
            "damerau_levenshtein_distance",
        ],
        var_name="comparator",
        value_name="score",
    )

    similarity_df = df_long[df_long["comparator"].str.contains("similarity")]
    similarity_records = similarity_df.to_json(orient="records")
    distance_df = df_long[df_long["comparator"].str.contains("distance")]
    distance_records = distance_df.to_json(orient="records")

    return _comparator_score_chart(similarity_records, distance_records)


def comparator_score_threshold_chart(
    dict, col1, col2, similarity_threshold=None, distance_threshold=None
):
    """
    Helper function returning a heatmap showing the sting similarity
    scores and string distances for a list of strings given a threshold.

    Args:
        dict (dictionary): Dictionary of string pairs to compare
        col1 (str):  The name of the first key in dictionary
        col2 (str):  The name of the second key in dictionary
        similarity_threshold (float): Threshold to set for similarity functions
        distance_threshold (int): Threshold to set for distance functions


    Examples:
        ```py
        import splink.comparison_helpers as ch

        data = {
                "string1": ["Stephen", "Stephen","Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.comparator_score_threshold_chart(data,
                                 "string1", "string2",
                                 similarity_threshold=0.8,
                                 distance_threshold=2)
        ```
    """
    df = comparator_score_df(list, col1, col2)

    df["strings_to_compare"] = df["string1"] + ", " + df["string2"]

    df_long = pd.melt(
        df,
        id_vars=["strings_to_compare"],
        value_vars=[
            "jaro_similarity",
            "jaro_winkler_similarity",
            "jaccard_similarity",
            "levenshtein_distance",
            "damerau_levenshtein_distance",
        ],
        var_name="comparator",
        value_name="score",
    )

    similarity_title = "Heatmap of Similarity Scores"
    distance_title = "Heatmap of Distance Scores"

    df_long["threshold_match"] = df_long.apply(
        lambda row: threshold_match(
            row["comparator"], row["score"], distance_threshold, similarity_threshold
        ),
        axis=1,
    )
    similarity_title = f"{similarity_title} with threshold {similarity_threshold}"
    distance_title = f"{distance_title} with threshold {distance_threshold}"

    records = df_long.to_json(orient="records")

    return _comparator_score_threshold_chart(
        records, similarity_threshold, distance_threshold
    )


def phonetic_transform(string):
    """
    Helper function to give the phonetic transformation of two strings with
    Soundex, Metaphone and Double Metaphone.

    Args:
        string (string): String to pass through phonetic transformation algorithms

    Examples:
        ```py
        phonetic_transform("Richard")
        ```
    """
    transforms = {}

    # Soundex Transform
    soundex_transform = phonetics.soundex(string)
    transforms["soundex"] = soundex_transform

    # Metaphone distance
    metaphone_transform = phonetics.metaphone(string)
    transforms["metaphone"] = metaphone_transform

    # Metaphone distance
    dmetaphone_transform = phonetics.dmetaphone(string)
    transforms["dmetaphone"] = dmetaphone_transform

    return transforms


def phonetic_transform_df(dict, col1, col2):
    """
    Helper function returning a dataframe showing the phonetic transforms
    for a dictionary of strings.

    Args:
        dict (dictionary): Dictionary of string pairs to compare
        col1 (str):  The name of the first key in dictionary
        col2 (str):  The name of the second key in dictionary

    Examples:
        ```py
        import splink.comparison_helpers as ch

        data = {
                "string1": ["Stephen", "Stephen","Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.phonetic_match_chart(data, "string1", "string2")
        ```
    """

    df = pd.DataFrame(list)

    df[f"soundex_{col1}"] = df.apply(lambda row: phonetics.soundex(row[col1]), axis=1)
    df[f"soundex_{col2}"] = df.apply(lambda row: phonetics.soundex(row[col2]), axis=1)
    df[f"metaphone_{col1}"] = df.apply(
        lambda row: phonetics.metaphone(row[col1]), axis=1
    )
    df[f"metaphone_{col2}"] = df.apply(
        lambda row: phonetics.metaphone(row[col2]), axis=1
    )
    df[f"dmetaphone_{col1}"] = df.apply(
        lambda row: phonetics.dmetaphone(row[col1]), axis=1
    )
    df[f"dmetaphone_{col2}"] = df.apply(
        lambda row: phonetics.dmetaphone(row[col2]), axis=1
    )

    df["soundex"] = df.apply(
        lambda x: [x[f"soundex_{col1}"], x[f"soundex_{col2}"]], axis=1
    )
    df["metaphone"] = df.apply(
        lambda x: [x[f"metaphone_{col1}"], x[f"metaphone_{col2}"]], axis=1
    )
    df["dmetaphone"] = df.apply(
        lambda x: [x[f"dmetaphone_{col1}"], x[f"dmetaphone_{col2}"]], axis=1
    )

    phonetic_df = df[[col1, col2, "soundex", "metaphone", "dmetaphone"]]

    return phonetic_df


def phonetic_match_chart(dict, col1, col2):
    """
    Helper function returning a heatmap showing the phonetic transform and
    matches for a dictionary of strings given a threshold.

    Args:
        dict (dictionary): Dictionary of string pairs to compare
        col1 (str):  The name of the first key in dictionary
        col2 (str):  The name of the second key in dictionary

    Examples:
        ```py
        import splink.comparison_helpers as ch

        data = {
                "string1": ["Stephen", "Stephen","Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.comparator_score_threshold_chart(data,
                                 "string1", "string2",
                                 similarity_threshold=0.8,
                                 distance_threshold=2)
        ```
    """

    df = phonetic_transform_df(list, "string1", "string2")

    df["strings_to_compare"] = df["string1"] + ", " + df["string2"]

    df_long = pd.melt(
        df,
        id_vars=["strings_to_compare"],
        value_vars=[
            "metaphone",
            "dmetaphone",
            "soundex",
        ],
        var_name="phonetic",
        value_name="transform",
    )
    df_long["match"] = df_long["transform"].apply(lambda x: x[0] == x[1])

    records = df_long.to_json(orient="records")

    return _phonetic_match_chart(records)
