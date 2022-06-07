import sqlglot
from sqlglot.expressions import Case, Alias
from itertools import groupby
from sqlglot.errors import ParseError


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
        sql = f"{sql}".format(sql=sql, lit=lit)

        parsed_case_expr.append(
            {"sql_expr": sql, "label": f"level_{lit}", "value": int(lit)}
        )

    if top_level_case_tree.args.get("default") is not None:
        lit = top_level_case_tree.args.get("default").sql("spark", pretty=True)
        sql = "ELSE"
        parsed_case_expr.append({"sql_expr": sql, "label": f"level_{lit}", "value": 0})

    return parsed_case_expr


def _merge_duplicate_levels(parsed_case_expr):
    def _join_or(groupby_item):

        items = list(groupby_item[1])

        exprs = [x["sql_expr"] for x in items]
        if len(exprs) > 1:

            exprs = [f"({e})" for e in exprs]
        merged = "\n OR ".join(exprs)

        if len(items) > 0:
            value = items[0]["value"]
        else:
            value = None
        return {"sql_condition": merged, "value": value}

    gb = groupby(parsed_case_expr, key=lambda x: x["label"])
    grouped_parsed = list(map(_join_or, gb))

    # Guarantee order and that -1 (null) comes first
    grouped_parsed = sorted(
        grouped_parsed, key=lambda x: -x["value"] if x["value"] >= 0 else -9999
    )

    return grouped_parsed


def _parse_case_statement(sql):

    tree = _get_top_level_case(sql)
    parsed = _parse_top_level_case_statement_from_sql(tree)

    # Finally dedupe - if two keys have the same level they need an OR condition
    return _merge_duplicate_levels(parsed)


def convert_settings_from_v2_to_v3(settings_dict_v2: dict):

    settings_3 = {}
    settings_3["blocking_rules_to_generate_predictions"] = settings_dict_v2[
        "blocking_rules"
    ]

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
        if k in settings_dict_v2:
            settings_3[k] = settings_dict_v2[k]

    comparisons_3 = []
    for comparison_column in settings_dict_v2["comparison_columns"]:

        parsed = _parse_case_statement(comparison_column["case_expression"])

        if "m_probabilities" in comparison_column:

            m = comparison_column["m_probabilities"]
            m.insert(0, None)
        else:
            m = [None] * (len(parsed) + 1)

        if "u_probabilities" in comparison_column:

            u = comparison_column["u_probabilities"]
            u.insert(0, None)
        else:
            u = [None] * (len(parsed) + 1)

        comparison_3 = {"comparison_levels": []}

        for index, level in enumerate(parsed):
            if level["value"] == -1:
                level["is_null_level"] = True
            if m[index]:
                level["m_probability"] = m[index]
            if u[index]:
                level["u_probability"] = u[index]
            del level["value"]
            comparison_3["comparison_levels"].append(level)

        comparisons_3.append(comparison_3)

    settings_3["comparisons"] = comparisons_3

    return settings_3
