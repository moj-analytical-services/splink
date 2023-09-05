import duckdb
import pandas as pd
import phonetics

from .charts import (
    _comparator_score_chart,
    _comparator_score_threshold_chart,
    _phonetic_match_chart,
)

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
    """Helper function to give the similarity between two strings for
    the string comparators in splink.

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


def comparator_score_df(list, col1, col2, decimal_places=2):
    """Helper function returning a dataframe showing the string similarity
    scores and string distances for a list of strings.

    Examples:
        ```py
        import splink.comparison_helpers as ch

        list = {
                "string1": ["Stephen", "Stephen","Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.comparator_score_df(list, "string1", "string2")
        ```
    """
    duckdb.connect()

    list = pd.DataFrame(list)

    sql = f"""
        select
        {col1}, {col2},
        {comparator_cols_sql.format(
            comparison1 = col1,
            comparison2 = col2,
            decimal_places=decimal_places
        )},
        from list
    """

    return duckdb.sql(sql).df()


def comparator_score_chart(list, col1, col2):
    """Helper function returning a heatmap showing the sting similarity
    scores and string distances for a list of strings.

    Examples:
        ```py
        import splink.comparison_helpers as ch

        list = {
                "string1": ["Stephen", "Stephen", "Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.comparator_score_chart(list, "string1", "string2")
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
    similarity_df.loc[:, "comparator"] = similarity_df["comparator"].str.replace(
        "_similarity", ""
    )
    similarity_records = similarity_df.to_json(orient="records")
    distance_df = df_long[df_long["comparator"].str.contains("distance")]
    distance_df.loc[:, "comparator"] = distance_df["comparator"].str.replace(
        "_distance", ""
    )
    distance_records = distance_df.to_json(orient="records")

    return _comparator_score_chart(similarity_records, distance_records)


def comparator_score_threshold_chart(
    list, col1, col2, similarity_threshold=None, distance_threshold=None
):
    """Helper function returning a heatmap showing the sting similarity
    scores and string distances for a list of strings given a threshold.

    Examples:
        ```py
        import splink.comparison_helpers as ch

        list = {
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

    similarity_df = df_long.loc[df_long["comparator"].str.contains("similarity"), :]
    similarity_df["comparator"] = similarity_df["comparator"].str.replace(
        "_similarity", ""
    )
    similarity_records = similarity_df.to_json(orient="records")
    distance_df = df_long.loc[df_long["comparator"].str.contains("distance"), :]
    distance_df["comparator"] = distance_df["comparator"].str.replace("_distance", "")
    distance_records = distance_df.to_json(orient="records")

    return _comparator_score_threshold_chart(
        similarity_records, distance_records, similarity_threshold, distance_threshold
    )


def phonetic_transform(string):
    """Helper function to give the phonetic transformation of two strings with
    Soundex, Metaphone and Double Metaphone.

    Examples:
        ```py
        phonetic_transform("Richard", "iRchard")
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


def phonetic_transform_df(list, col1, col2):
    """Helper function returning a dataframe showing the phonetic transforms
    for a list of strings.

    Examples:
        ```py
        import splink.comparison_helpers as ch

        list = {
                "string1": ["Stephen", "Stephen","Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.phonetic_match_chart(list, "string1", "string2")
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


def phonetic_match_chart(list, col1, col2):
    """Helper function returning a heatmap showing the phonetic transform and
    matches for a list of strings given a threshold.

    Examples:
        ```py
        import splink.comparison_helpers as ch

        list = {
                "string1": ["Stephen", "Stephen","Stephen"],
                "string2": ["Stephen", "Steven", "Stephan"],
                }

        ch.comparator_score_threshold_chart(list,
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
