import sqlglot
from sqlglot.errors import ParseError
from sqlglot.expressions import Case, Column, Alias
import re


def get_columns_used_from_sql(sql):
    column_names = set()
    syntax_tree = sqlglot.parse_one(sql, read="spark")
    for tup in syntax_tree.walk():
        subtree = tup[0]
        if type(subtree) is Column:
            column_names.add(subtree.sql())

    return list(column_names)


def get_columns_used_from_sql_without_l_r_suffix(sql):
    cols = get_columns_used_from_sql(sql)
    cols = [re.sub(r"_L$|_R$", "", c, flags=re.IGNORECASE) for c in cols]
    return list(set(cols))


def _tree_is_alias(syntax_tree):
    return type(syntax_tree) is Alias


def _tree_is_case(syntax_tree):
    return type(syntax_tree) is Case


def _get_top_level_case(sql):
    try:
        syntax_tree = sqlglot.parse_one(sql, read="spark")
    except ParseError as e:
        raise ValueError(f"Error parsing case statement:\n{sql}") from e

    if _tree_is_alias(syntax_tree):
        case = syntax_tree.find(Case)
        if case.depth == 1:
            sql = case.sql()
            case_tree = sqlglot.parse_one(sql, read="spark")
            return case_tree
        else:
            raise ValueError(
                "Error parsing case statement - no case statement found at top level\n"
                f"Statement was: {sql}"
            )
    elif _tree_is_case(syntax_tree):
        return syntax_tree
    else:
        raise ValueError(
            "Error parsing case statement - no case statement found at top level\n"
            f"Statement was: {sql}"
        )


def _parse_top_level_case_statement_from_sql(top_level_case_tree):

    parsed_case_expr = []

    ifs = top_level_case_tree.args["ifs"]
    for i in ifs:
        lit = i.args["true"].sql()

        sql = i.args["this"].sql(dialect="spark")
        sql = f"WHEN {sql} THEN {lit}".format(sql=sql, lit=lit)

        parsed_case_expr.append({"sql_expr": sql, "label": f"level_{lit}"})

    if top_level_case_tree.args.get("default") is not None:
        lit = top_level_case_tree.args.get("default").sql("spark", pretty=True)
        sql = f"ELSE {lit}"
        parsed_case_expr.append({"sql_expr": sql, "label": f"level_{lit}"})

    return parsed_case_expr


def parse_case_statement(sql):

    tree = _get_top_level_case(sql)
    return _parse_top_level_case_statement_from_sql(tree)


def generate_sql_from_parsed_case_expr(parsed_case_expr, col_name=None):
    sql = "CASE\n"
    for value in parsed_case_expr:
        sql_expr = value["sql_expr"]
        sql += f"    {sql_expr}\n"

    sql += "END"
    if col_name:
        sql += f" AS gamma_{col_name}"
    return sql
