import math


def jc_sim_py(str1, str2):
    """
    Jaccard`similarity calculated exactly as in stringutils.similarity jaccard in Apache Commons
    """

    if not str1 or not str2:
        return 0.0

    k = 2  # default k in stringutil is 2 so leaving it like that for compatibility

    # break strings into sets of rolling k-char syllables
    a = set([str1[i : i + 1] for i in range(len(str1) - k + 1)])
    b = set([str2[i : i + 1] for i in range(len(str2) - k + 1)])

    # calculate instersection of two sets
    c = a.intersection(b)

    # return Jaccard similarity
    return float(len(c)) / (len(a) + len(b) - len(c))


def jw_sim_py(
    first,
    second,  # modification from original to not use other imput parameters
):
    """
    Jaro-Winkler similarity calculated exactly as in stringutils.similarity_jaro_winkler in Apache Commons
    using a modified version of the algorithm implemented by 'Jean-Bernard Ratte - jean.bernard.ratte@unary.ca'
    found at https://github.com/nap/jaro-winkler-distance

    used under the Apache License, Version 2.0 , with modifictions explicitly marked
    """

    if not first or not second:
        return 0.0  # modification from original to give 0.0 in case of nulls instead of a an exception

    scaling = 0.1  # modification from original to have constant scaling in order to have a comparable result to the stringutils implementation

    def _get_diff_index(first, second):
        if first == second:
            return -1

        if not first or not second:
            return 0

        max_len = min(len(first), len(second))
        for i in range(0, max_len):
            if not first[i] == second[i]:
                return i

        return max_len

    def _score(first, second):
        shorter, longer = first.lower(), second.lower()

        if len(first) > len(second):
            longer, shorter = shorter, longer

        m1 = _get_matching_characters(shorter, longer)
        m2 = _get_matching_characters(longer, shorter)

        if len(m1) == 0 or len(m2) == 0:
            return 0.0

        return (
            float(len(m1)) / len(shorter)
            + float(len(m2)) / len(longer)
            + float(len(m1) - _transpositions(m1, m2)) / len(m1)
        ) / 3.0

    def _get_prefix(first, second):
        if not first or not second:
            return ""

        index = _get_diff_index(first, second)
        if index == -1:
            return first

        elif index == 0:
            return ""

        else:
            return first[0:index]

    def _transpositions(first, second):
        return math.floor(
            len([(f, s) for f, s in zip(first, second) if not f == s]) / 2.0
        )

    def _get_matching_characters(first, second):
        common = []
        limit = math.floor(min(len(first), len(second)) / 2)

        for i, l in enumerate(first):
            left, right = int(max(0, i - limit)), int(min(i + limit + 1, len(second)))
            if l in second[left:right]:
                common.append(l)
                second = (
                    second[0 : second.index(l)] + "*" + second[second.index(l) + 1 :]
                )

        return "".join(common)

    jaro = _score(first, second)
    cl = min(len(_get_prefix(first, second)), 4)

    return round((jaro + (scaling * cl * (1.0 - jaro))) * 100.0) / 100.0
