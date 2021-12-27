import sqlglot
from sqlglot.expressions import Case, Column, Alias
import re


def get_columns_used_from_expr(expr):
    column_names = []
    for tup in expr.walk():
        item, parent, key = tup
        if type(item) is Column:
            column_names.append(item.sql())
    return column_names


def tree_is_alias(syntax_tree):
    return type(syntax_tree) is Alias


def tree_is_case(syntax_tree):
    return type(syntax_tree) is Case


def get_top_level_case(syntax_tree):
    if tree_is_alias(syntax_tree):
        case = syntax_tree.find(Case)
        if case.depth == 1:
            sql = case.sql()
            case_tree = sqlglot.parse_one(sql, read="spark")
            return case_tree
        else:
            raise ValueError(
                "Error parsing case statement" " - no case statement found at top level"
            )
    if tree_is_case(syntax_tree):
        return syntax_tree


def parse_top_level_case_statement_from_sql(top_level_case_tree):

    parsed_dict = {}

    ifs = top_level_case_tree.args["ifs"]
    for i in ifs:
        lit = i.args["true"].sql()

        # sql = i.args["this"].sql()
        sql = i.sql()
        # remove leading "CASE " and trailing " END"
        sql = re.sub(r"^CASE ", "", sql)
        sql = re.sub(r" END$", "", sql)
        parsed_dict[lit] = sql

    if top_level_case_tree.args.get("default") is not None:
        lit = top_level_case_tree.args.get("default").sql()
        sql = f"ELSE {lit}"
        parsed_dict[lit] = sql

    return parsed_dict


def parse_case_statement(sql):
    tree = sqlglot.parse_one(sql, read="spark")
    tree = get_top_level_case(tree)
    return parse_top_level_case_statement_from_sql(tree)


# def parse_else_value(item):
#     res = {}
#     case_args = item.args
#     else_stmt = case_args["default"]
#     else_stmt_value = else_stmt.this

#     res["literal_value"] = else_stmt_value
#     res["sql"] = f"ELSE {else_stmt_value} END"
#     res["cols_used"] = []
#     return res


# def parse_if_value(item):
#     res = {}
#     case_args = item.args
#     if_stmt_value = case_args["true"].this

#     res["literal_value"] = if_stmt_value
#     sql = item.sql()

#     import re

#     sql = re.sub(r"^CASE ", "", sql)

#     sql = re.sub(r" END$", "", sql)

#     res["sql"] = sql
#     cols_used = get_columns_used_from_expr(item)
#     res["cols_used"] = cols_used
#     return res


# def parse_case_statement(sql_expression):

#     # Python 3.7 and CPython 3.6 dicts guaranteed to iterate in order of insertion
#     case_statement_definition = {}

#     # Should use parse here just to check not more than one?
#     parsed = sqlglot.parse_one(sql_expression)

#     for tup in parsed.walk():

#         item, parent, key = tup

#         if type(item) is Case:
#             all_cols_used = get_columns_used_from_expr(item)

#             case_args = item.args

#             ifs = case_args["ifs"]
#             for this_if in ifs:

#                 if_dict = parse_if_value(this_if)
#                 val = if_dict["literal_value"]
#                 case_statement_definition[val] = if_dict

#             else_dict = parse_else_value(item)
#             val = else_dict["literal_value"]
#             case_statement_definition[val] = else_dict

#     return case_statement_definition
