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


def interpolate(start, end, num_elements):
    steps = num_elements - 1
    step = (end - start) / steps
    vals = [start + (i * step) for i in range(0, num_elements)]
    return vals


def normalise(vals):
    return [v / sum(vals) for v in vals]


def m_u_records_to_lookup_dict(m_u_records):
    lookup = {}
    for m_u_record in m_u_records:
        comparison_name = m_u_record["comparison_name"]
        level_value = m_u_record["comparison_vector_value"]
        if comparison_name not in lookup:
            lookup[comparison_name] = {}
        if level_value not in lookup[comparison_name]:
            lookup[comparison_name][level_value] = {}

        m_prob = m_u_record["m_probability"]

        u_prob = m_u_record["u_probability"]

        if m_prob is not None:
            lookup[comparison_name][level_value]["m_probability"] = m_prob
        if u_prob is not None:
            lookup[comparison_name][level_value]["u_probability"] = u_prob

    return lookup
