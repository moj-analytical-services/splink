from unittest.mock import patch

import duckdb
import pytest

import splink.internals.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import Linker, SettingsCreator
from splink.internals.blocking_rule_library import block_on
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.input_column import InputColumn
from splink.internals.pipeline import CTEPipeline


def _comparison_vector_sqls(
    *,
    input_tablename_l="nodes",
    input_tablename_r="nodes",
    link_type="dedupe_only",
    sql_dialect_str="duckdb",
    composite_ids=False,
):
    source_dataset_column = (
        InputColumn("source_dataset", sqlglot_dialect_str="duckdb")
        if composite_ids
        else None
    )
    unique_id_column = InputColumn("unique_id", sqlglot_dialect_str="duckdb")
    columns = [
        'l."unique_id" as "unique_id_l"',
        'r."unique_id" as "unique_id_r"',
        'l."value" as "value_l"',
        'r."value" as "value_r"',
    ]
    if composite_ids:
        columns = [
            'l."source_dataset" as "source_dataset_l"',
            'r."source_dataset" as "source_dataset_r"',
            *columns,
        ]

    return compute_comparison_vector_values_from_id_pairs_sqls(
        columns_to_select_for_blocking=columns,
        columns_to_select_for_comparison_vector_values=["*"],
        input_tablename_l=input_tablename_l,
        input_tablename_r=input_tablename_r,
        source_dataset_input_column=source_dataset_column,
        unique_id_input_column=unique_id_column,
        link_type=link_type,
        sql_dialect_str=sql_dialect_str,
    )


@pytest.mark.parametrize(
    "link_type",
    ["dedupe_only", "link_only", "link_and_dedupe", "two_dataset_link_only"],
)
def test_separate_inputs_are_joined_directly_for_all_link_types(link_type):
    sqls = _comparison_vector_sqls(
        input_tablename_l="nodes_l",
        input_tablename_r="nodes_r",
        link_type=link_type,
    )

    assert len(sqls) == 2
    assert "nodes_l as l" in sqls[0]["sql"]
    assert "nodes_r as r" in sqls[0]["sql"]
    assert all("__splink__df_concat_with_tf_filtered" not in sql["sql"] for sql in sqls)


@pytest.mark.parametrize("dialect", ["duckdb", "spark", "sqlite", "postgres"])
def test_only_duckdb_blocked_pair_node_ctes_are_not_materialized(dialect):
    pipeline = CTEPipeline()
    hint_node_ctes = dialect == "duckdb"
    node_ctes = [
        ("select * from input_nodes", "__splink__df_concat"),
        (
            "select * from __splink__df_concat "
            "where id in (select join_key_l from pairs)",
            "__splink__df_concat_filtered_l",
        ),
        (
            "select * from __splink__df_concat "
            "where id in (select join_key_r from pairs)",
            "__splink__df_concat_filtered_r",
        ),
        (
            "select * from __splink__df_concat_filtered_l",
            "__splink__df_concat_with_tf_l",
        ),
        (
            "select * from __splink__df_concat_filtered_r",
            "__splink__df_concat_with_tf_r",
        ),
    ]
    for cte_sql, cte_name in node_ctes:
        pipeline.enqueue_sql(cte_sql, cte_name, duckdb_not_materialized=hint_node_ctes)
    pipeline.enqueue_sql(
        "select * from __splink__df_concat_with_tf",
        "__splink__df_comparison_vectors",
    )
    pipeline.enqueue_sql(
        "select * from __splink__df_comparison_vectors", "__splink__df_predict"
    )

    sql = pipeline.generate_cte_pipeline_sql()

    if dialect == "duckdb":
        assert sql.count("as not materialized (") == 5
    else:
        assert "not materialized" not in sql
    assert "__splink__df_comparison_vectors as (" in sql


def test_direct_node_joins_preserve_composite_id_results():
    connection = duckdb.connect()
    connection.execute(
        """
        create table nodes as
        select
            case when record_id % 2 = 0 then 'left' else 'right' end
                as source_dataset,
            record_id as unique_id,
            record_id % 17 as value
        from range(20000) records(record_id)
        """
    )
    connection.execute(
        """
        create table __splink__blocked_id_pairs as
        select
            'left-__-0' as join_key_l,
            'right-__-' || cast(2 * pair_id + 1 as varchar) as join_key_r,
            cast(pair_id % 3 as varchar) as match_key
        from range(1000) pairs(pair_id)
        """
    )

    sqls = _comparison_vector_sqls(composite_ids=True)
    joined_rows = connection.execute(sqls[0]["sql"]).fetchall()

    assert len(joined_rows) == 1000


def _predict_registered_pairs(*, link_type, with_tf, use_hints, empty=False):
    left_nodes = [
        {"unique_id": 0, "value": "a", "value_2": "x"},
        {"unique_id": 1, "value": "b", "value_2": "y"},
    ]
    right_nodes = [
        {"unique_id": 0, "value": "a", "value_2": "x"},
        {"unique_id": 1, "value": "b", "value_2": "z"},
        {"unique_id": 2, "value": "a", "value_2": "y"},
    ]
    if link_type == "dedupe_only":
        blocked_pairs = [
            {"join_key_l": 0, "join_key_r": 2, "match_key": "0"},
            {"join_key_l": 0, "join_key_r": 3, "match_key": "0"},
            {"join_key_l": 1, "join_key_r": 2, "match_key": "0"},
            {"join_key_l": 1, "join_key_r": 4, "match_key": "0"},
        ]
    else:
        blocked_pairs = [
            {
                "join_key_l": "input_left-__-0",
                "join_key_r": "input_right-__-0",
                "match_key": "0",
            },
            {
                "join_key_l": "input_left-__-0",
                "join_key_r": "input_right-__-1",
                "match_key": "0",
            },
            {
                "join_key_l": "input_left-__-1",
                "join_key_r": "input_right-__-0",
                "match_key": "0",
            },
            {
                "join_key_l": "input_left-__-1",
                "join_key_r": "input_right-__-2",
                "match_key": "0",
            },
        ]
    if with_tf:
        comparisons = [
            cl.CustomComparison(
                output_column_name=column,
                comparison_levels=[
                    cll.NullLevel(column),
                    cll.ExactMatchLevel(column).configure(tf_adjustment_column=column),
                    cll.ElseLevel(),
                ],
            )
            for column in ("value", "value_2")
        ]
    else:
        comparisons = [cl.ExactMatch("value")]
    settings = SettingsCreator(
        link_type=link_type,
        comparisons=comparisons,
        blocking_rules_to_generate_predictions=[block_on("value")],
        source_dataset_column_name="source_dataset",
    )

    db_api = DuckDBAPI()
    if link_type == "dedupe_only":
        nodes = [
            {"unique_id": index, "value": row["value"], "value_2": row["value_2"]}
            for index, row in enumerate([*left_nodes, *right_nodes])
        ]
        linker = Linker(db_api.register(nodes), settings)
    else:
        left_sdf = db_api.register(left_nodes, dataset_display_name="input_left")
        right_sdf = db_api.register(right_nodes, dataset_display_name="input_right")
        linker = Linker([left_sdf, right_sdf], settings)
    if empty:
        key_type = "bigint" if link_type == "dedupe_only" else "varchar"
        blocked_pairs_sdf = db_api.register(
            db_api.duckdb_con.sql(
                f"select cast(null as {key_type}) as join_key_l, "
                f"cast(null as {key_type}) as join_key_r, "
                "cast(null as varchar) as match_key where false"
            )
        )
    else:
        blocked_pairs_sdf = db_api.register(blocked_pairs)
    linker.table_management.register_blocked_pairs_for_predict(blocked_pairs_sdf)

    if use_hints:
        predictions = linker.inference.predict(warning_mode="never")
    else:
        from splink.internals.vertically_concatenate import enqueue_df_concat_with_tf

        def enqueue_shared_tf_lineage(linker, pipeline):
            enqueue_df_concat_with_tf(linker, pipeline)
            pipeline.enqueue_sql(
                "select * from __splink__df_concat_with_tf",
                "__splink__df_concat_with_tf_l",
            )
            pipeline.enqueue_sql(
                "select * from __splink__df_concat_with_tf",
                "__splink__df_concat_with_tf_r",
            )
            return pipeline

        with patch(
            "splink.internals.linker_components.inference."
            "enqueue_duckdb_df_concat_with_tf_for_blocked_pairs",
            side_effect=enqueue_shared_tf_lineage,
        ):
            predictions = linker.inference.predict(warning_mode="never")

    records = predictions.query_sql(
        "select * from {this} order by unique_id_l, unique_id_r"
    ).as_record_list()
    return records, predictions.sql_used_to_create, db_api


@pytest.mark.parametrize("link_type", ["dedupe_only", "link_only", "link_and_dedupe"])
@pytest.mark.parametrize("with_tf", [False, True])
def test_separate_prefiltered_node_ctes_preserve_complete_prediction_output(
    link_type, with_tf
):
    hinted, sql, _ = _predict_registered_pairs(
        link_type=link_type, with_tf=with_tf, use_hints=True
    )
    unhinted, _, _ = _predict_registered_pairs(
        link_type=link_type, with_tf=with_tf, use_hints=False
    )

    assert hinted == unhinted
    assert len(hinted) == 4
    assert sql.count("as not materialized (") == 5
    assert "__splink__df_concat_filtered_l as not materialized (" in sql
    assert "__splink__df_concat_filtered_r as not materialized (" in sql
    assert "select join_key_l" in sql
    assert "select join_key_r" in sql
    assert "__splink__df_concat_with_tf_l as not materialized (" in sql
    assert "__splink__df_concat_with_tf_r as not materialized (" in sql
    assert sql.index("__splink__df_concat_filtered_l as") < sql.index(
        "__splink__df_concat_with_tf_l as"
    )
    assert sql.index("__splink__df_concat_filtered_r as") < sql.index(
        "__splink__df_concat_with_tf_r as"
    )
    assert "__splink__df_concat_with_tf as" not in sql
    assert "__splink__df_concat_with_tf_filtered" not in sql
    assert all("match_weight" in row and "match_probability" in row for row in hinted)


@pytest.mark.parametrize("with_tf", [False, True])
def test_empty_registered_pairs_predicts_no_rows(with_tf):
    records, sql, _ = _predict_registered_pairs(
        link_type="dedupe_only", with_tf=with_tf, use_hints=True, empty=True
    )

    assert records == []
    assert sql.count("as not materialized (") == 5


def test_duckdb_plan_inlines_two_independent_node_scans():
    _, sql, db_api = _predict_registered_pairs(
        link_type="dedupe_only", with_tf=True, use_hints=True
    )

    plan = db_api.duckdb_con.execute("explain " + sql).fetchone()[1]

    assert plan.count("HASH_JOIN") == 8
    assert "unique_id = join_key_l" in plan
    assert "join_key_r = unique_id" in plan
    assert "Join Type: SEMI" in plan
    assert "tf_value" in plan
    assert "CTE" not in plan
    assert "CTE_SCAN" not in plan
    assert "__common_subplan_" not in plan
    assert "__splink__df_concat_with_tf_filtered" not in plan


def test_constrained_memory_registered_pairs_prediction(tmp_path):
    connection = duckdb.connect()
    connection.execute("set threads = 1")
    connection.execute("set memory_limit = '128MB'")
    connection.execute(f"set temp_directory = '{tmp_path.as_posix()}'")
    connection.execute(
        """
        create table nodes as
        select node_id as unique_id, node_id % 1000 as value
        from range(100000) as nodes(node_id)
        """
    )
    connection.execute(
        """
        create table blocked_pairs as
        select
            pair_id as join_key_l,
            pair_id + 1 as join_key_r,
            '0' as match_key
        from range(50000) as pairs(pair_id)
        """
    )
    db_api = DuckDBAPI(connection=connection)
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[cl.ExactMatch("value")],
        blocking_rules_to_generate_predictions=[block_on("value")],
    )
    linker = Linker(db_api.register(connection.table("nodes")), settings)
    blocked_pairs = db_api.register(connection.table("blocked_pairs"))
    linker.table_management.register_blocked_pairs_for_predict(blocked_pairs)

    predictions = linker.inference.predict(warning_mode="never")
    row_count = predictions.as_duckdbpyrelation().count("*").fetchone()[0]
    plan = connection.execute("explain " + predictions.sql_used_to_create).fetchone()[1]

    assert row_count == 50000
    assert plan.count("HASH_JOIN") == 4
    assert "Join Type: SEMI" in plan
    assert "CTE" not in plan
    assert "CTE_SCAN" not in plan
