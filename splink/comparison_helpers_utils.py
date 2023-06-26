import re


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
