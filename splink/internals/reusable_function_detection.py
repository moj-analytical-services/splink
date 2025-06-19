from __future__ import annotations

from typing import Dict, Set, Tuple

from sqlglot import exp, parse_one


def _count_repeated_functions(ast: exp.Expression, sqlglot_dialect: str) -> Set[str]:
    """Return a set of function SQL strings that appear more than once."""
    function_counts: Dict[str, int] = {}

    # First pass: count all functions
    for func in ast.find_all(exp.Func):
        func_sql = func.sql(dialect=sqlglot_dialect)
        function_counts[func_sql] = function_counts.get(func_sql, 0) + 1

    # Second pass: remove nested functions that are part of repeated outer functions
    for func in ast.find_all(exp.Func):
        func_sql = func.sql(dialect=sqlglot_dialect)
        # If this function is repeated
        if function_counts.get(func_sql, 0) > 1:
            # Check if it's nested inside another repeated function
            parent = func.parent
            while parent:
                if isinstance(parent, exp.Func):
                    parent_sql = parent.sql(dialect=sqlglot_dialect)
                    if function_counts.get(parent_sql, 0) > 1:
                        # Remove this nested function from results
                        function_counts[func_sql] = 1
                        break
                parent = parent.parent

    return {f for f, count in function_counts.items() if count > 1}


def _replace_repeated_functions(
    ast: exp.Expression,
    repeated_funcs: Set[str],
    var_mapping: Dict[str, str],
    sqlglot_dialect: str,
) -> exp.Expression:
    """Replace repeated function calls with references to computed columns."""

    def transform_func(node):
        if isinstance(node, exp.Func):
            node_sql = node.sql(dialect=sqlglot_dialect)
            if node_sql in repeated_funcs:
                # Skip if nested in another repeated function
                if (
                    node.parent
                    and isinstance(node.parent, exp.Func)
                    and node.parent.sql(dialect=sqlglot_dialect) in repeated_funcs
                ):
                    return node
                # Return reference to computed column
                return exp.to_identifier(var_mapping[node_sql])
        return node

    return ast.transform(transform_func)


def _find_repeated_functions(
    columns_to_select_for_comparison_vector_values: list[str],
    sqlglot_dialect: str,
) -> Tuple[list[dict[str, str]], list[str]]:
    """Find repeated function calls and return both the function definitions
    and modified SQL that uses the computed values.

    Args:
        columns_to_select_for_comparison_vector_values: List of SQL expressions
        sqlglot_dialect: SQLGlot dialect name to use for parsing and generating SQL

    Returns:
        Tuple containing:
        - List of dicts with function_sql and alias for repeated functions
        - List of modified SQL strings with references to computed values
    """
    repeated_functions = []
    modified_columns = []

    # Only look at CASE statements
    case_statements = [
        col
        for col in columns_to_select_for_comparison_vector_values
        if col.startswith("CASE")
    ]

    # First pass: find all repeated functions
    all_repeated_funcs = set()
    for case_sql in case_statements:
        ast = parse_one(case_sql, read=sqlglot_dialect)
        all_repeated_funcs.update(_count_repeated_functions(ast, sqlglot_dialect))

    # Build mapping of function SQL to alias
    var_mapping = {}
    for func_sql in all_repeated_funcs:
        alias = "".join(c if c.isalnum() else "_" for c in func_sql.lower())
        var_mapping[func_sql] = alias
        repeated_functions.append({"function_sql": func_sql, "alias": alias})

    # Second pass: replace function calls with column references
    for col in columns_to_select_for_comparison_vector_values:
        if col.startswith("CASE"):
            ast = parse_one(col, read=sqlglot_dialect)
            modified_ast = _replace_repeated_functions(
                ast, all_repeated_funcs, var_mapping, sqlglot_dialect
            )
            modified_columns.append(modified_ast.sql(dialect=sqlglot_dialect))
        else:
            modified_columns.append(col)

    return repeated_functions, modified_columns


def _build_reusable_functions_sql(repeated_functions: list[dict[str, str]]) -> str:
    """Build SQL to compute reusable function values."""
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
