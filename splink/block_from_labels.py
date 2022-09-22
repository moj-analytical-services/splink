from typing import TYPE_CHECKING
from .lower_id_on_lhs import lower_id_to_left_hand_side

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def block_from_labels(
    linker: "Linker", labels_table_name: str, include_clerical_match_score=False
):
    """Create pairwise record comparisons corresponding to the ID pairs in a labels
    table

    The table of labels should be in the following format, and should be registered
    with your database:

    |source_dataset_l|unique_id_l|source_dataset_r|unique_id_r|clerical_match_score|
    |----------------|-----------|----------------|-----------|--------------------|
    |df_1            |1          |df_2            |2          |0.99                |
    |df_1            |1          |df_2            |3          |0.2                 |

    Note that `source_dataset` and `unique_id` should correspond to the values
    specified in the settings dict, and the `input_table_aliases` passed to the
    `linker` object.
    """

    df = linker._table_to_splink_dataframe(labels_table_name, labels_table_name)

    unique_id_col = linker._settings_obj._unique_id_column_name

    source_dataset_col = linker._settings_obj._source_dataset_column_name

    sql = lower_id_to_left_hand_side(df, source_dataset_col, unique_id_col)

    sqls = []
    sql = {
        "sql": sql,
        "output_table_name": "__splink__labels_prepared_for_joining",
    }

    sqls.append(sql)

    columns_to_select = linker._settings_obj._columns_to_select_for_blocking
    sql_select_expr = ", ".join(columns_to_select)

    if linker._settings_obj._source_dataset_column_name_is_required:
        join_condition_l = f"""
        l.{source_dataset_col} = df_labels.{source_dataset_col}_l and
        l.{unique_id_col} = df_labels.{unique_id_col}_l
        """
        join_condition_r = f"""
        r.{source_dataset_col} = df_labels.{source_dataset_col}_r and
        r.{unique_id_col} = df_labels.{unique_id_col}_r
        """
    else:
        join_condition_l = f"l.{unique_id_col} = df_labels.{unique_id_col}_l"
        join_condition_r = f"r.{unique_id_col} = df_labels.{unique_id_col}_r"

    if include_clerical_match_score:
        clerical_match_score = ", clerical_match_score"
    else:
        clerical_match_score = ""

    sql = f"""
    select
        {sql_select_expr},
        'from_labels' as match_key
        {clerical_match_score}


    from
    __splink__labels_prepared_for_joining as df_labels
    inner join __splink__df_concat_with_tf as l
    on {join_condition_l}
    inner join __splink__df_concat_with_tf as r
    on {join_condition_r}
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_blocked",
    }

    sqls.append(sql)

    return sqls
