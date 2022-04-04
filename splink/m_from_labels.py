from .comparison_vector_values import compute_comparison_vector_values
from .maximisation_step import compute_new_parameters
from .block_from_labels import block_from_labels


def estimate_m_from_pairwise_labels(linker, table_name):

    sqls = block_from_labels(linker, table_name)

    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])

    sql = compute_comparison_vector_values(linker.settings_obj)

    linker.enqueue_sql(sql, "__splink__df_comparison_vectors")

    sql = """
    select *, 1.0D as match_probability
    from __splink__df_comparison_vectors
    """
    linker.enqueue_sql(sql, "__splink__df_predict")

    sql = compute_new_parameters(linker.settings_obj)
    linker.enqueue_sql(sql, "__splink__df_new_params")

    df_params = linker.execute_sql_pipeline()

    param_records = df_params.as_record_dict()

    m_u_records = [
        r for r in param_records if r["comparison_name"] != "_proportion_of_matches"
    ]

    for record in m_u_records:
        cc = linker.settings_obj._get_comparison_by_name(record["comparison_name"])
        gamma_val = record["comparison_vector_value"]
        cl = cc.get_comparison_level_by_comparison_vector_value(gamma_val)

        cl.add_trained_m_probability(
            record["m_probability"], "estimate m from pairwise labels"
        )
    linker.populate_m_u_from_trained_values()
