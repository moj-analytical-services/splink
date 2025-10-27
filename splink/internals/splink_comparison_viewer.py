from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

from splink.internals.misc import EverythingEncoder, read_resource

from .predict import _combine_prior_and_bfs

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from splink.internals.linker import Linker


def row_examples(
    linker: Linker, example_rows_per_category: int = 2
) -> list[dict[str, str]]:
    sqls = []

    uid_cols = linker._settings_obj.column_info_settings.unique_id_input_columns
    uid_cols_l = [uid_col.name_l for uid_col in uid_cols]
    uid_cols_r = [uid_col.name_r for uid_col in uid_cols]
    uid_col_lr_names = uid_cols_l + uid_cols_r
    uid_expr = " || '-' ||".join(uid_col_lr_names)

    gamma_columns = [c._gamma_column_name for c in linker._settings_obj.comparisons]

    gam_concat = " || ',' || ".join(gamma_columns)

    # See https://github.com/moj-analytical-services/splink/issues/1651
    # This ensures we have an average match weight that isn't affected by tf
    bf_columns_no_tf = [c._bf_column_name for c in linker._settings_obj.comparisons]

    p = linker._settings_obj._probability_two_random_records_match
    bf_final_no_tf = _combine_prior_and_bfs(
        p,
        bf_terms=bf_columns_no_tf,
        sql_infinity_expr=linker._infinity_expression,
        sql_dialect=linker._db_api.sql_dialect,
    )[0]

    sql = f"""
    select
        *,
        {uid_expr} as rec_comparison_id,
        {gam_concat} as gam_concat,
        log2({bf_final_no_tf}) as sort_avg_match_weight,
        random() as rand_order
    from __splink__df_predict
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_predict_with_row_id",
    }
    sqls.append(sql_info)

    sql = """
    select *,
        ROW_NUMBER() OVER (PARTITION BY gam_concat order by rand_order)
            AS row_example_index
    from __splink__df_predict_with_row_id
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_predict_with_row_num",
    }
    sqls.append(sql_info)

    sql = f"""
    select *
    from __splink__df_predict_with_row_num
    where row_example_index <= {example_rows_per_category}
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_example_rows",
    }

    sqls.append(sql_info)

    return sqls


def comparison_viewer_table_sqls(
    linker: Linker,
    example_rows_per_category: int = 2,
    minimum_comparison_vector_count: int = 0,
) -> list[dict[str, str]]:
    sqls = row_examples(linker, example_rows_per_category)

    sql = f"""
    select ser.*,
           cvd.sum_gam,
           cvd.count_rows_in_comparison_vector_group as count,
           cvd.proportion_of_comparisons
    from __splink__df_example_rows as ser
    left join
     __splink__df_comparison_vector_distribution as cvd
    on ser.gam_concat = cvd.gam_concat
    where cvd.count_rows_in_comparison_vector_group >= {minimum_comparison_vector_count}
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_comparison_viewer_table",
    }

    sqls.append(sql_info)
    return sqls


def render_splink_comparison_viewer_html(
    comparison_vector_data: list[dict[str, Any]],
    splink_settings: dict[str, Any],
    out_path: str,
    overwrite: bool = False,
) -> str:
    from jinja2 import Template

    # When developing the package, it can be easier to point
    # ar the script live on observable using <script src=>
    # rather than bundling the whole thing into the html
    bundle_observable_notebook = True

    template_path = "internals/files/splink_comparison_viewer/template.j2"
    template = Template(read_resource(template_path))

    template_data: dict[str, Any] = {
        "comparison_vector_data": json.dumps(
            comparison_vector_data, cls=EverythingEncoder
        ),
        "splink_settings": json.dumps(splink_settings),
    }

    files = {
        "embed": "internals/files/external_js/vega-embed@6.20.2",
        "vega": "internals/files/external_js/vega@5.31.0",
        "vegalite": "internals/files/external_js/vega-lite@5.2.0",
        "stdlib": "internals/files/external_js/stdlib.js@5.8.3",
        "svu_text": "internals/files/splink_vis_utils/splink_vis_utils.js",
        "custom_css": "internals/files/splink_comparison_viewer/custom.css",
    }
    for k, v in files.items():
        template_data[k] = read_resource(v)

    template_data["bundle_observable_notebook"] = bundle_observable_notebook

    rendered = template.render(**template_data)

    if os.path.isfile(out_path) and not overwrite:
        raise ValueError(
            f"The path {out_path} already exists. Please provide a different path."
        )
    else:
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            dbutils.fs.put(out_path, rendered, overwrite=True)
            # to view the dashboard inline in notebook displayHTML(rendered)
            return rendered
        else:
            with open(out_path, "w", encoding="utf-8") as html_file:
                html_file.write(rendered)
            # return the rendered dashboard html for inline viewing in the notebook
            return rendered
