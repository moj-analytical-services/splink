import logging
from random import randint

import pandas as pd

from .cost_of_blocking_rules import calculate_cost_of_combination_of_brs

logger = logging.getLogger(__name__)


def localised_shuffle(lst: list, window_percent: float) -> list:
    """
    Performs a localised shuffle on a list.

    This is used to choose semi-randomly from a list of
    sorted rows, so you tend to pick from items towards the top

    Args:
        lst (list): The list to shuffle.
        window_percent (float): The window percent for shuffle e.g. 0.3 for shuffle
            within 30% of orig position

    Returns:
        list: A shuffled copy of the original list.
    """
    window_size = max(1, int(window_percent * len(lst)))
    return sorted(lst, key=lambda x: lst.index(x) + randint(-window_size, window_size))


def check_field_freedom(candidate_set, field_names, min_field_freedom):
    """
    Checks if each field in the candidate set is allowed to vary at least
    'min_field_freedom' times.

    Args:
        candidate_set (list): The candidate set of rows.
        field_names (list): The list of field names.
        min_field_freedom (int): The minimum field freedom.

    Returns:
        bool: True if each field can vary at least 'min_field_freedom' times,
            False otherwise.
    """
    covered_fields = {field: 0 for field in field_names}
    for row in candidate_set:
        for field in field_names:
            if row[field] == 0:
                covered_fields[field] += 1
    return all(count >= min_field_freedom for count in covered_fields.values())


def heuristic_select_brs_that_have_min_freedom(data, field_names, min_field_freedom):
    """
    A heuristic algorithm to select blocking rules that between them
    ensure that each field is allowed to vary at least 'min_field_freedom' times.

    Args:
        data (list): The data rows.
        field_names (list): The list of field names.
        min_field_freedom (int): The minimum field freedom.

    Returns:
        list: The candidate set of rows.
    """
    data_sorted_randomised = localised_shuffle(data, 0.5)
    candidate_rows = []

    for row in data_sorted_randomised:
        candidate_rows.append(row)
        if check_field_freedom(candidate_rows, field_names, min_field_freedom):
            break

    sorted_candidate_rows = sorted(
        candidate_rows, key=lambda x: x["blocking_columns_sanitised"]
    )

    return sorted_candidate_rows


def get_block_on_string(br_rows):
    block_on_strings = []

    for row in br_rows:
        quoted_args = []
        for arg in row["blocking_columns_sanitised"]:
            quoted_arg = f'"{arg}"'
            quoted_args.append(quoted_arg)

        block_on_args = ", ".join(quoted_args)
        block_on_string = f"block_on([{block_on_args}])"
        block_on_strings.append(block_on_string)

    return " \n".join(block_on_strings)


def get_em_training_string(br_rows):
    block_on_strings = []

    for row in br_rows:
        quoted_args = []
        for arg in row["blocking_columns_sanitised"]:
            quoted_arg = f'"{arg}"'
            quoted_args.append(quoted_arg)

        block_on_args = ", ".join(quoted_args)
        block_on_string = f"block_on([{block_on_args}])"
        block_on_strings.append(block_on_string)

    training_statements = []
    for block_on_str in block_on_strings:
        statement = (
            f"linker.estimate_parameters_using_expectation_maximisation({block_on_str})"
        )
        training_statements.append(statement)

    return " \n".join(training_statements)


def suggest_blocking_rules(
    df_block_stats,
    min_freedom=1,
    num_runs=100,
    num_equi_join_weight=0,
    field_freedom_weight=1,
    num_brs_weight=10,
    num_comparison_weight=10,
):
    """Use a cost optimiser to suggest blocking rules

    Args:
        df_block_stats: Dataframe returned by find_blocking_rules_below_threshold
        min_freedom (int, optional): Each column should have at least this many
            opportunities to vary amongst the blocking rules. Defaults to 1.
        num_runs (int, optional): How many random combinations of
            rules to try.  The best will be selected. Defaults to 5.
        num_equi_join_weight (int, optional): The weight for number of equi joins.
            Defaults to 0.
        field_freedom_weight (int, optional): The weight for field freedom. Defaults to
            10.
        num_brs_weight (int, optional): The weight for the number of blocking rules
            found. Defaults to 10.

    Returns:
        pd.DataFrame: A DataFrame containing the results of the blocking rules
            suggestion. It includes columns such as
            'suggested_blocking_rules_for_prediction',
            'suggested_EM_training_statements', and various cost information

    """
    if len(df_block_stats) == 0:
        return None

    max_comparison_count = df_block_stats["comparison_count"].max()

    df_block_stats = df_block_stats.sort_values(
        by=["num_equi_joins", "comparison_count"], ascending=[True, False]
    )
    blocks_found_recs = df_block_stats.to_dict(orient="records")

    blocking_cols = list(blocks_found_recs[0].keys())
    blocking_cols = [c for c in blocking_cols if c.startswith("__fixed__")]

    results = []

    for run in range(num_runs):
        selected_rows = heuristic_select_brs_that_have_min_freedom(
            blocks_found_recs, blocking_cols, min_field_freedom=min_freedom
        )

        cost_dict = {
            "suggested_blocking_rules_for_prediction": get_block_on_string(
                selected_rows
            ),
            "suggested_EM_training_statements": get_em_training_string(selected_rows),
        }

        costs = calculate_cost_of_combination_of_brs(
            selected_rows,
            max_comparison_count,
            num_equi_join_weight,
            field_freedom_weight,
            num_brs_weight,
            num_comparison_weight,
        )

        cost_dict.update(costs)
        cost_dict.update(
            {
                "run_num": run,
                "minimum_freedom_for_each_column": min_freedom,
                "suggested_blocking_rules_as_splink_brs": [
                    row["splink_blocking_rule"] for row in selected_rows
                ],
            }
        )
        results.append(cost_dict)

    results_df = pd.DataFrame(results)
    # easier to read if we normalise the cost so the best is 0
    min_ = results_df["field_freedom_cost"].min()
    results_df["field_freedom_cost"] = results_df["field_freedom_cost"] - min_

    min_ = results_df["field_freedom_cost_weighted"].min()
    results_df["field_freedom_cost_weighted"] = (
        results_df["field_freedom_cost_weighted"] - min_
    )
    results_df["cost"] = results_df["cost"] - min_

    min_scores_df = results_df.sort_values("cost")
    min_scores_df = min_scores_df.drop_duplicates(
        "suggested_blocking_rules_for_prediction"
    )

    return min_scores_df
