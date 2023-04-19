import jellyfish


def get_distance_metrics(str1, str2):
    """Helper function to give the similarity between two strings for
    the string comparators in splink.

    Examples:
        >>> get_distance_metrics("Richard", "iRchard")
    """

    distances = {}

    # Levenshtein distance
    lev_dist = jellyfish.levenshtein_distance(str1, str2)
    distances["levenshtein_distance"] = round(lev_dist, 3)

    # Damerau-Levenshtein distance
    dlev_dist = jellyfish.damerau_levenshtein_distance(str1, str2)
    distances["damerau_levenshtein_distance"] = round(dlev_dist, 3)

    # Hamming distance
    ham_dist = jellyfish.hamming_distance(str1, str2)
    distances["hamming_distance"] = round(ham_dist, 3)

    # Jaro distance
    jaro_dist = jellyfish.jaro_distance(str1, str2)
    distances["jaro_distance"] = round(jaro_dist, 3)

    # Jaro-Winkler distance
    jw_dist = jellyfish.jaro_winkler(str1, str2)
    distances["jaro_winkler"] = round(jw_dist, 3)

    # Jaccard similarity

    def jaccard_similarity(str1, str2):
        set1 = set(str1)
        set2 = set(str2)
        return len(set1 & set2) / len(set1 | set2)

    jaccard_sim = jaccard_similarity(str1, str2)
    distances["jaccard_similarity"] = round(jaccard_sim, 3)

    return distances
