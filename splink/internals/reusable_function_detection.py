from typing import List


def _find_repeated_functions(
    columns_to_select_for_comparison_vector_values: list[str],
) -> list[dict[str, str]]:
    """Find repeated function calls in the comparison vector columns.
    For now, just hardcoded to look for specific patterns.

    Returns a list of dicts with keys:
        - function_sql: the original function call
        - alias: the column name to store the result
    """
    repeated_functions = []

    # For now, just hardcode the patterns we're looking for
    patterns = [
        {
            "function_sql": 'jaro_winkler_similarity("first_name_l", "first_name_r")',
            "alias": "jw_first_name",
        },
        {"function_sql": 'jaccard("surname_l", "surname_r")', "alias": "jd_surname"},
    ]

    # Check if any of our patterns appear in the CASE statements
    for col in columns_to_select_for_comparison_vector_values:
        if not col.startswith("CASE"):
            continue

        for pattern in patterns:
            if pattern["function_sql"] in col:
                repeated_functions.append(pattern)

    return repeated_functions


def _build_reusable_functions_sql(repeated_functions: list[dict[str, str]]) -> str:
    """Build SQL to compute reusable function values.
    Takes list of function definitions from _find_repeated_functions.
    """
    if not repeated_functions:
        return "select * from blocked_with_cols"

    computed_columns = [
        f"{func['function_sql']} as {func['alias']}" for func in repeated_functions
    ]

    sql = f"""
    select *,
           {','.join(computed_columns)}
    from blocked_with_cols
    """
    return sql
