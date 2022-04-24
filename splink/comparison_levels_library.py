from .input_column import InputColumn


def null_level(col_name):
    col = InputColumn(col_name)
    return {
        "sql_condition": f"{col.name_l()} IS NULL OR {col.name_r()} IS NULL",
        "label_for_charts": "Null",
        "is_null_level": True,
    }


def exact_match_level(col_name, m_probability=None, term_frequency_adjustments=False):
    col = InputColumn(col_name)
    d = {
        "sql_condition": f"{col.name_l()} = {col.name_r()}",
        "label_for_charts": "Exact match",
    }
    if m_probability:
        d["m_probability"] = m_probability
    if term_frequency_adjustments:
        d["tf_adjustment_column"] = col_name

    return d


def levenshtein_level(col_name, distance_threshold, m_probability=None):
    col = InputColumn(col_name)
    sql_cond = f"levenshtein({col.name_l()}, {col.name_r()}) <= {distance_threshold}"
    d = {
        "sql_condition": sql_cond,
        "label_for_charts": f"Levenstein <= {distance_threshold}",
    }
    if m_probability:
        d["m_probability"] = m_probability

    return d


def else_level(
    m_probability=None,
):
    d = {
        "sql_condition": "ELSE",
        "label_for_charts": "All other comparisons",
    }
    if m_probability:
        d["m_probability"] = m_probability
    return d
