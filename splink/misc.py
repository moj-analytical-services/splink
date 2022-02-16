def dedupe_preserving_order(list_of_items):
    return list(dict.fromkeys(list_of_items))


def escape_columns(cols):
    return [escape_column(c) for c in cols]


def escape_column(col):
    return f"`{col}`"


def prob_to_bayes_factor(prob):
    return prob / (1 - prob)


def bayes_factor_to_prob(bf):
    return bf / (1 + bf)
