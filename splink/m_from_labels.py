from copy import deepcopy

from .blocking import block_using_rules
from .comparison_vector_values import compute_comparison_vector_values
from .maximisation_step import compute_new_parameters
from .lower_id_on_lhs import lower_id_to_left_hand_side


def estimate_m_from_pairwise_labels(linker, table_name):

    original_settings_object = linker.settings_obj
    training_linker = deepcopy(linker)
    settings_obj_for_training = training_linker.settings_obj

    settings_obj_for_training._link_type = "link_only"
    training_linker.train_m_from_pairwise_labels_mode = True

    df = training_linker._df_as_obj(table_name, table_name)
    source_dataset_col = settings_obj_for_training._source_dataset_column_name
    unique_id_col = settings_obj_for_training._unique_id_column_name

    sql = lower_id_to_left_hand_side(df, source_dataset_col, unique_id_col)

    training_linker.enqueue_sql(sql, "__splink__labels_prepared_for_joining")

    settings_obj_for_training._blocking_rules_to_generate_predictions = []

    columns_to_select = settings_obj_for_training._columns_to_select_for_blocking
    sql_select_expr = ", ".join(columns_to_select)

    sql = f"""
    select {sql_select_expr} from
    __splink__labels_prepared_for_joining as df_labels
    inner join __splink__df_concat_with_tf as l
    on l.{source_dataset_col} = df_labels.{source_dataset_col}_l and l.{unique_id_col} = df_labels.{unique_id_col}_l
    inner join __splink__df_concat_with_tf as r
    on r.{source_dataset_col} = df_labels.{source_dataset_col}_r and r.{unique_id_col} = df_labels.{unique_id_col}_r

    """

    training_linker.enqueue_sql(sql, "__splink__df_blocked")

    sql = compute_comparison_vector_values(settings_obj_for_training)

    training_linker.enqueue_sql(sql, "__splink__df_comparison_vectors")

    sql = """
    select *, 1.0D as match_probability
    from __splink__df_comparison_vectors
    """
    training_linker.enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters(settings_obj_for_training)
    training_linker.enqueue_sql(sql, "__splink__df_new_params")

    df_params = linker.execute_sql_pipeline()

    param_records = df_params.as_record_dict()

    m_u_records = [
        r for r in param_records if r["comparison_name"] != "_proportion_of_matches"
    ]

    for record in m_u_records:
        cc = original_settings_object._get_comparison_by_name(record["comparison_name"])
        gamma_val = record["comparison_vector_value"]
        cl = cc.get_comparison_level_by_comparison_vector_value(gamma_val)

        cl.add_trained_m_probability(
            record["m_probability"], "estimate m from pairwise labels"
        )
    linker.populate_m_u_from_trained_values()
