# reusable_function_detection.py
from __future__ import annotations

from collections import Counter
from typing import Dict, List, Tuple

from sqlglot import exp, parse_one


def _find_repeated_functions(
    columns_to_select_for_comparison_vector_values: List[str],
    sqlglot_dialect: str,
) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Detect function sub-expressions that are used more than once inside CASE
    expressions and rewrite the columns so they refer to computed aliases.

    Returns
    -------
    repeated_functions : list of {"function_sql": str, "alias": str}
        One entry per *root* duplicate function, in deterministic order.
    modified_columns   : list[str]
        The input columns, with duplicates replaced by their alias.
    """
    # --- 1. Parse only the CASE expressions ---------------------------------
    case_asts = [
        parse_one(col, read=sqlglot_dialect)
        for col in columns_to_select_for_comparison_vector_values
        if col.lstrip().upper().startswith("CASE")
    ]

    # --- 2. Count every function node across all CASEs ----------------------
    func_counts: Counter[exp.Expression] = Counter()
    for ast in case_asts:
        func_counts.update(fn for fn in ast.find_all(exp.Func))

    repeated: set[exp.Expression] = {fn for fn, c in func_counts.items() if c > 1}

    # --- 3. Keep only the *root* duplicates (not nested inside another dup) --
    def is_nested_in_repeated(fn: exp.Func) -> bool:
        parent = fn.parent
        while parent:
            if isinstance(parent, exp.Func) and parent in repeated:
                return True
            parent = parent.parent
        return False

    roots_in_order: list[exp.Func] = []
    seen: set[exp.Expression] = set()  # protect against re-adding same struct
    for ast in case_asts:
        for fn in ast.find_all(exp.Func):
            if fn in repeated and not is_nested_in_repeated(fn) and fn not in seen:
                roots_in_order.append(fn)
                seen.add(fn)

    # --- 4. Give each root an alias -----------------------------------------
    var_mapping: Dict[exp.Expression, str] = {}
    repeated_functions: list[dict[str, str]] = []

    for idx, fn in enumerate(roots_in_order, start=1):
        alias = f"rf_{idx}"
        var_mapping[fn] = alias
        repeated_functions.append(
            {
                "function_sql": fn.sql(dialect=sqlglot_dialect),
                "alias": alias,
            }
        )

    # --- 5. Replace in every CASE AST ---------------------------------------
    def _replace(node: exp.Expression) -> exp.Expression:
        # Only root duplicates are in var_mapping
        if isinstance(node, exp.Func) and node in var_mapping:
            return exp.to_identifier(var_mapping[node])
        return node

    modified_columns: list[str] = []
    for col in columns_to_select_for_comparison_vector_values:
        if col.lstrip().upper().startswith("CASE"):
            ast = parse_one(col, read=sqlglot_dialect)
            modified_columns.append(
                ast.transform(_replace).sql(dialect=sqlglot_dialect)
            )
        else:
            modified_columns.append(col)

    return repeated_functions, modified_columns


def _build_reusable_functions_sql(repeated_functions: List[Dict[str, str]]) -> str:
    """
    Build a CTE that adds all reusable columns to `blocked_with_cols`.
    """
    if not repeated_functions:
        return "SELECT * FROM blocked_with_cols"

    computed_cols = ",\n           ".join(
        f"{f['function_sql']} AS {f['alias']}" for f in repeated_functions
    )
    return f"""SELECT *,
           {computed_cols}
    FROM blocked_with_cols"""
