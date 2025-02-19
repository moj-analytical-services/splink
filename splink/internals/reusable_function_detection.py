from typing import Dict, List, Set

from sqlglot import exp, parse_one


def _count_repeated_functions(ast: exp.Expression, dialect: str = "duckdb") -> Set[str]:
    """Return a set of function SQL strings that appear more than once."""
    function_counts: Dict[str, int] = {}
    for func in ast.find_all(exp.Func):
        func_sql = func.sql(dialect=dialect)
        function_counts[func_sql] = function_counts.get(func_sql, 0) + 1
    return {f for f, count in function_counts.items() if count > 1}


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

    # Only look at CASE statements
    case_statements = [
        col
        for col in columns_to_select_for_comparison_vector_values
        if col.startswith("CASE")
    ]

    # Parse each CASE statement and find repeated functions
    for case_sql in case_statements:
        ast = parse_one(case_sql, read="duckdb")
        repeated_funcs = _count_repeated_functions(ast)

        for func_sql in repeated_funcs:
            # Generate a clean alias from the function name
            alias = "".join(c if c.isalnum() else "_" for c in func_sql.lower())
            repeated_functions.append({"function_sql": func_sql, "alias": alias})

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
