import re

import altair as alt
import jellyfish
import pandas as pd
import phonetics


def comparator_score(str1, str2):
    """Helper function to give the similarity between two strings for
    the string comparators in splink.

    Examples:
        >>> comparator_score("Richard", "iRchard")
    """

    scores = {}

    # Levenshtein distance
    lev_dist = jellyfish.levenshtein_distance(str1, str2)
    scores["levenshtein_distance"] = round(lev_dist, 3)

    # Damerau-Levenshtein distance
    dlev_dist = jellyfish.damerau_levenshtein_distance(str1, str2)
    scores["damerau_levenshtein_distance"] = round(dlev_dist, 3)

    # Jaro distance
    jaro_sim = jellyfish.jaro_distance(str1, str2)
    scores["jaro_similarity"] = round(jaro_sim, 3)

    # Jaro-Winkler distance
    jw_sim = jellyfish.jaro_winkler(str1, str2)
    scores["jaro_winkler_similarity"] = round(jw_sim, 3)

    # Jaccard similarity

    def jaccard_similarity(str1, str2):
        set1 = set(str1)
        set2 = set(str2)
        return len(set1 & set2) / len(set1 | set2)

    jaccard_sim = jaccard_similarity(str1, str2)
    scores["jaccard_similarity"] = round(jaccard_sim, 3)

    return scores


def distance_match(distance, threshold):
    if distance <= threshold:
        return True
    else:
        return False


def similarity_match(similarity, threshold):
    if similarity >= threshold:
        return True
    else:
        return False


def threshold_match(comparator, score, distance_threshold, similarity_threshold):
    if re.search("distance", comparator):
        return distance_match(score, distance_threshold)
    elif re.search("similarity", comparator):
        return similarity_match(score, similarity_threshold)


def comparator_score_df(list, col1, col2):
    
    df = pd.DataFrame(list)
    
    scores = []

    for _index, row in df.iterrows():
        str1 = row[col1]
        str2 = row[col2]
        row_scores = comparator_score(str1, str2)
        row_scores["string1"] = str1
        row_scores["string2"] = str2
        scores.append(row_scores)

    scores_df = pd.DataFrame(
        scores,
        columns=[
            "string1",
            "string2",
            "levenshtein_distance",
            "damerau_levenshtein_distance",
            "jaro_similarity",
            "jaro_winkler_similarity",
            "jaccard_similarity",
        ],
    )

    return scores_df


def comparator_score_chart(
    list, col1, col2, similarity_threshold=None, distance_threshold=None
):
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
    graph_colour = "score"

    if similarity_threshold and distance_threshold:
        df_long["threshold_match"] = df_long.apply(
            lambda row: threshold_match(
                row["comparator"],
                row["score"],
                distance_threshold,
                similarity_threshold,
            ),
            axis=1,
        )
        similarity_title = f"{similarity_title} with threshold {similarity_threshold}"
        distance_title = f"{distance_title} with threshold {distance_threshold}"
        graph_colour = "threshold_match"

    # create similarity heatmap
    heatmap_similarity = (
        alt.Chart(
            df_long[df_long["comparator"].str.contains("similarity")],
            width=500,
            height=300,
        )
        .mark_rect()
        .encode(
            x="comparator:O",
            y="strings_to_compare:O",
            color=alt.Color(
                f"{graph_colour}:Q",
                scale=alt.Scale(range=["red", "green"], domain=[0, 1]),
            ),
        )
        .properties(title=similarity_title)
    )

    text_similarity = heatmap_similarity.mark_text(baseline="middle").encode(
        text=alt.Text("score:Q", format=".2f"),
        color=alt.condition(
            alt.datum.quantity > 3, alt.value("white"), alt.value("black")
        ),
    )

    similarity_scores = heatmap_similarity + text_similarity

    # create distance heatmap
    heatmap_distance = (
        alt.Chart(
            df_long[df_long["comparator"].str.contains("distance")],
            width=500,
            height=300,
        )
        .mark_rect()
        .encode(
            x="comparator:O",
            y="strings_to_compare:O",
            color=alt.Color(
                f"{graph_colour}:Q",
                scale=alt.Scale(range=["green", "red"], domain=[0, 5]),
            ),
        )
        .properties(title=distance_title)
    )

    text_distance = heatmap_distance.mark_text(baseline="middle").encode(
        text=alt.Text("score:Q"),
        color=alt.condition(
            alt.datum.quantity > 3, alt.value("white"), alt.value("black")
        ),
    )

    distance_scores = heatmap_distance + text_distance

    # show heatmap
    scores_chart = alt.hconcat(similarity_scores, distance_scores).resolve_scale(
        color="independent"
    )

    return scores_chart


def comparator_score_threshold_chart(
    df, col1, col2, similarity_threshold=None, distance_threshold=None
):
    df = comparator_score_df(df, col1, col2)

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

    # create match heatmap
    heatmap_match = (
        alt.Chart(df_long, width=500, height=300)
        .mark_rect()
        .encode(
            x="comparator:O",
            y="strings_to_compare:O",
            color=alt.Color(
                "threshold_match:O", scale=alt.Scale(range=["red", "green"])
            ),
        )
        .properties(
            title=f"Heatmap of Matches for distance_threshold = {distance_threshold},"
            f"similarity_threshold = {similarity_threshold}"
        )
    )

    text_match = heatmap_match.mark_text(baseline="middle").encode(
        text="score:O",
        color=alt.condition(
            alt.datum.quantity > 3, alt.value("white"), alt.value("black")
        ),
    )

    matches = heatmap_match + text_match
    return matches


def phonetic_transform(string):
    """Helper function to give the phonetic transformation of two strings with
    Soundex, Metaphone and Double Metaphone.

    Examples:
        >>> phonetic_transform("Richard", "iRchard")
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


def phonetic_match(string1, string2):
    """ """
    phonetic1 = phonetic_transform(string1)
    phonetic2 = phonetic_transform(string2)

    comparison_list = [
        transform1 == transform2 for transform1, transform2 in zip(phonetic1, phonetic2)
    ]

    return comparison_list


def phonetic_match_chart(df, col1, col2):

    df = phonetic_transform_df(df, "string1", "string2")

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

    # create match heatmap
    heatmap_phonetic_match = (
        alt.Chart(df_long, width=500, height=600)
        .mark_rect()
        .encode(
            x="phonetic:O",
            y="strings_to_compare:O",
            color=alt.Color("match:O", scale=alt.Scale(range=["red", "green"])),
        )
        .properties(title="Heatmap of Phonetic Matches")
    )

    text_phonetic_match = heatmap_phonetic_match.mark_text(baseline="middle").encode(
        text="transform:O",
        color=alt.condition(
            alt.datum.quantity > 3, alt.value("white"), alt.value("black")
        ),
    )

    phonetic_matches = heatmap_phonetic_match + text_phonetic_match

    return phonetic_matches
