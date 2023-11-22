import logging
import string
from typing import TYPE_CHECKING, Dict, List, Set

import pandas as pd

from .blocking import BlockingRule
from .input_column import InputColumn

if TYPE_CHECKING:
    from .linker import Linker
logger = logging.getLogger(__name__)


def sanitise_column_name_for_one_hot_encoding(column_name) -> str:
    allowed_chars = string.ascii_letters + string.digits + "_"
    sanitised_name = "".join(c for c in column_name if c in allowed_chars)
    return sanitised_name


def _generate_output_combinations_table_row(
    blocking_columns, splink_blocking_rule, comparison_count, all_columns
) -> dict:
    row = {}

    blocking_columns = [
        sanitise_column_name_for_one_hot_encoding(c) for c in blocking_columns
    ]
    all_columns = [sanitise_column_name_for_one_hot_encoding(c) for c in all_columns]

    row["blocking_columns_sanitised"] = blocking_columns
    row["splink_blocking_rule"] = splink_blocking_rule
    row["comparison_count"] = comparison_count
    row["num_equi_joins"] = len(blocking_columns)

    for col in all_columns:
        row[f"__fixed__{col}"] = 1 if col in blocking_columns else 0

    return row


def _generate_combinations(
    all_columns, current_combination, already_visited: Set[frozenset]
) -> list:
    """Generate combinations of columns to visit that haven't been visited already
    irrespective of order
    """

    combinations = []
    for col in all_columns:
        if col not in current_combination:
            next_combination = current_combination + [col]
            if frozenset(next_combination) not in already_visited:
                combinations.append(next_combination)

    return combinations


def _generate_blocking_rule(
    linker: "Linker", cols_as_string: List[str]
) -> BlockingRule:
    """Generate a Splink blocking rule given a list of column names which
    are provided as as string"""

    # TODO: Refactor in Splink4
    dialect = linker._sql_dialect

    module_mapping = {
        "presto": "splink.athena.blocking_rule_library",
        "duckdb": "splink.duckdb.blocking_rule_library",
        "postgres": "splink.postgres.blocking_rule_library",
        "spark": "splink.spark.blocking_rule_library",
        "sqlite": "splink.sqlite.blocking_rule_library",
    }

    if dialect not in module_mapping:
        raise ValueError(f"Unsupported SQL dialect: {dialect}")

    module_name = module_mapping[dialect]
    block_on_module = __import__(module_name, fromlist=["block_on"])
    block_on = block_on_module.block_on

    if len(cols_as_string) == 0:
        return block_on("1")

    br = block_on(cols_as_string)

    return br


def _search_tree_for_blocking_rules_below_threshold_count(
    linker: "Linker",
    all_columns: List[str],
    threshold: float,
    current_combination: List[str] = None,
    already_visited: Set[frozenset] = None,
    results: List[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
    """
    Recursively search combinations of fields to find ones that result in a count less
    than the threshold.

    Uses the new, fast counting function
    linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions
    to count

    The full tree looks like this, where c1 c2 are columns:
    c1                    count_comparisons(c1)
    ├── c2                count_comparisons(c1, c2)
    │   └── c3            count_comparisons(c1, c2, c3)
    ├── c3                count_comparisons(c1, c3)
    │   └── c2            count_comparisons(c1, c3, c2)
    c2                    count_comparisons(c2)
    ├── c1                count_comparisons(c2, c1)
    │   └── c3            count_comparisons(c2, c1, c3)
    ├── c3                count_comparisons(c2, c3)
    │   └── c1            count_comparisons(c2, c3, c1)

    But many nodes do not need to be visited:
        - Once the count is below the threshold, no branches from the node are explored.
        - If a combination has alraedy been evaluated, it is not evaluated again. For
          example, c2 -> c1 will not be evaluated because c1 -> c2 has already been
          counted

    When a count is below the threshold, create a dictionary with the relevant stats
    like :
    {
        'blocking_columns_sanitised':['first_name'],
        'splink_blocking_rule':<Custom rule>',
        comparison_count':4827,
        'num_equi_join':1,
        '__fixed__first_name':1,
        '__fixed__surname':0,
        '__fixed__dob':0,
        '__fixed__city':0,
        '__fixed__email':0,
        '__fixed__cluster':0,
    }

    Return a list of these dicts.


    Args:
        linker: splink.Linker
        fields (List[str]): List of fields to combine.
        threshold (float): The count threshold.
        current_combination (List[str], optional): Current combination of fields.
        already_visited (Set[frozenset], optional): Set of visited combinations.
        results (List[Dict[str, str]], optional): List of results. Defaults to [].

    Returns:
        List[Dict]: List of results.  Each result is a dict with statistics like
            the number of comparisons, the blocking rule etc.
    """
    if current_combination is None:
        current_combination = []
    if already_visited is None:
        already_visited = set()
    if results is None:
        results = []

    if len(current_combination) == len(all_columns):
        return results  # All fields included, meaning we're at a leaf so exit recursion

    br = _generate_blocking_rule(linker, current_combination)
    comparison_count = (
        linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(br)
    )

    already_visited.add(frozenset(current_combination))

    if comparison_count > threshold:
        # Generate all valid combinations and continue the search
        combinations = _generate_combinations(
            all_columns, current_combination, already_visited
        )
        for next_combination in combinations:
            _search_tree_for_blocking_rules_below_threshold_count(
                linker,
                all_columns,
                threshold,
                next_combination,
                already_visited,
                results,
            )
    else:
        row = _generate_output_combinations_table_row(
            current_combination,
            br,
            comparison_count,
            all_columns,
        )
        results.append(row)

    return results


def find_blocking_rules_below_threshold_comparison_count(
    linker: "Linker", max_comparisons_per_rule, column_expressions: List[str] = None
) -> pd.DataFrame:
    """
    Finds blocking rules which return a comparison count below a given threshold.

    In addition to returning blocking rules, returns the comparison count and
    'num_equi_joins', which refers to the number of equi-joins used by the rule.

    Also returns one-hot encoding that describes which columns are __fixed__ by the
    blocking rule

    e.g. equality on first_name and surname has num_equi_joins of 2

    Args:
        linker (Linker): The Linker object
        max_comparisons_per_rule (int): Max comparisons allowed per blocking rule.
        column_expressions: List[str] = Algorithm will find combinations of these
            column expressions to use as blocking rules. If None, uses all columns used
            by the ComparisonLevels of the Linker. Column expressions can be SQL
            expressions, not just column names i.e. 'substr(surname, 1,1)' is a valid
            entry in this list.

    Returns:
        pd.DataFrame: DataFrame with blocking rules, comparison_count and num_equi_joins
    """

    if not column_expressions:
        column_expressions = linker._input_columns(
            include_unique_id_col_names=False,
            include_additional_columns_to_retain=False,
        )

    column_expressions_as_strings = []

    for c in column_expressions:
        if isinstance(c, InputColumn):
            column_expressions_as_strings.append(c.quote().name)
        else:
            column_expressions_as_strings.append(c)

    results = _search_tree_for_blocking_rules_below_threshold_count(
        linker, column_expressions_as_strings, max_comparisons_per_rule
    )

    if not results:
        raise ValueError(
            "No blocking rules could be found that produce a comparison count below "
            "your chosen max_comparisons_per_rule threshold of "
            f"{max_comparisons_per_rule}. Try increasing the threshold."
        )

    return pd.DataFrame(results)
