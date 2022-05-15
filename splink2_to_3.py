import urllib.request
import json
import re
import sqlglot
from sqlglot.expressions import Case, Column, Alias, Bracket, Lambda
from itertools import groupby
from sqlglot.errors import ParseError


url = "https://gist.githubusercontent.com/RobinL/1101d5c919de39d9435983d0edce5aa2/raw/d1428393a9a161317f693cd277aaae60b66d0d4f/splink_settings.json"
with urllib.request.urlopen(url) as response:
    data = response.read()


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


def _merge_duplicate_levels(parsed_case_expr):
    def _join_or(groupby_item):

        exprs = [x["sql_expr"] for x in groupby_item[1]]
        if len(exprs) > 1:

            exprs = [f"({e})" for e in exprs]
        merged = "\n OR ".join(exprs)
        return merged

    gb = groupby(parsed_case_expr, key=lambda x: x["label"])
    return list(map(_join_or, gb))


def parse_case_statement(sql):

    tree = _get_top_level_case(sql)
    parsed = _parse_top_level_case_statement_from_sql(tree)

    # Finally dedupe - if two keys have the same level they need an OR condition
    return _merge_duplicate_levels(parsed)


settings_2 = json.loads(data)
settings_3 = {}
settings_3["blocking_rules_to_generate_predictions"] = settings_2["blocking_rules"]

copy_keys = [
    "link_type",
    "proportion_of_matches",
    "max_iterations",
    "em_convergence",
    "unique_id_column_name",
    "source_dataset_column_name",
    "retain_matching_columns",
    "retain_intermediate_calculation_columns",
    "additional_columns_to_retain",
]

for k in copy_keys:
    if k in settings_2:
        settings_3[k] = settings_2[k]

comparisons_3 = []
for comparison_column in settings_2["comparison_columns"]:

    m = comparison_column["m_probabilities"]
    m.insert(0, None)
    u = comparison_column["u_probabilities"]
    u.insert(0, None)
    parsed = parse_case_statement(comparison_column["case_expression"])
    m_u_ps = list(zip(m, u, parsed))
    comparison_3 = {"comparison_levels": []}

    for m, u, p in m_u_ps:
        level = {"m_probability": m, "u_probability": u, "sql_condition": p}
        if m is None:
            del level["m_probability"]
        if u is None:
            del level["u_probability"]
        if m is None:
            level["is_null_level"] = True
        comparison_3["comparison_levels"].append(level)
    comparisons_3.append(comparison_3)
settings_3["comparisons"] = comparisons_3

import json

# print(json.dumps(settings_3, indent=4))

from splink.settings import Settings

s = Settings(settings_3)
