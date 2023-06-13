import splink.comparison_helpers as ch
import splink.duckdb.comparison_template_library as ctl
import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_level_library as cll

def test_get_comparison_levels():
    names = ["Julia", "Julia", "Julie", "Rachel"]

    ctl_comparison = ctl.name_comparison("name")
    ch.get_comparison_levels(names, ctl_comparison)

    cl_comparison = cl.levenshtein_at_thresholds("name", 2)
    ch.get_comparison_levels(names, cl_comparison)

    cll_comparison = {
        "output_column_name": "name",
        "comparison_levels": [
            cll.null_level("name"),
            cll.exact_match_level("name"),
            cll.levenshtein_level("name", 2),
            cll.else_level(),
        ],
    }
    ch.get_comparison_levels(names, cll_comparison)

    dict_comparison = {
        "output_column_name": "name",
        "comparison_levels": [
            {
                "sql_condition": "name_l IS NULL OR name_r IS NULL",
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            {
                "sql_condition": "name_l = name_r",
                "label_for_charts": "Exact match",
            },
            {
                "sql_condition": "levenshtein(name_l, name_r) < 2",
                "label_for_charts": "Exact match",
            },
            {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
        ],
    }
    ch.get_comparison_levels(names, dict_comparison)