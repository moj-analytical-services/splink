from jinja2 import Template
import json
import os
import pkgutil
from typing import TYPE_CHECKING, List
from .misc import EverythingEncoder

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def row_examples(linker: "Linker", example_rows_per_category=2):

    sqls = []

    uid_cols = linker._settings_obj._unique_id_input_columns
    uid_cols_l = [uid_col.name_l() for uid_col in uid_cols]
    uid_cols_r = [uid_col.name_r() for uid_col in uid_cols]
    uid_cols = uid_cols_l + uid_cols_r
    uid_expr = " || '-' ||".join(uid_cols)

    gamma_columns = [c._gamma_column_name for c in linker._settings_obj.comparisons]

    gam_concat = " || ',' || ".join(gamma_columns)

    sql = f"""
    select
        *,
        {uid_expr} as rec_comparison_id,
        {gam_concat} as gam_concat,
        random() as rand_order
    from __splink__df_predict
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_predict_with_row_id",
    }
    sqls.append(sql)

    sql = """
    select *,
        ROW_NUMBER() OVER (PARTITION BY gam_concat order by rand_order)
            AS row_example_index
    from __splink__df_predict_with_row_id
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_predict_with_row_num",
    }
    sqls.append(sql)

    sql = f"""
    select *
    from __splink__df_predict_with_row_num
    where row_example_index <= {example_rows_per_category}
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_example_rows",
    }

    sqls.append(sql)

    return sqls


# def row_examples_correlated_subquery()


def comparison_viewer_table_sqls(
    linker: "Linker", example_rows_per_category=2
) -> List[dict]:

    sqls = row_examples(linker, example_rows_per_category)

    sql = """
    select ser.*,
           cvd.sum_gam,
           cvd.count_rows_in_comparison_vector_group,
           cvd.proportion_of_comparisons
    from __splink__df_example_rows as ser
    left join
     __splink__df_comparison_vector_distribution as cvd
    on ser.gam_concat = cvd.gam_concat
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_comparison_viewer_table",
    }

    sqls.append(sql)
    return sqls

    # Correlated subquey approach to getting row examples does not work in DuckDB
    # since the limit keyword doesn't seem to work as expected

    # sql = f"""
    # select *
    # from __splink__df_predict_with_row_id as p1
    # where  rec_comparison_id in
    #     (select rec_comparison_id
    #     from __splink__df_predict_with_row_id
    #     where gam_concat
    #           = p1.gam_concat

    #     limit 1)
    # """

    # sql = {
    #     "sql": sql,
    #     "output_table_name": "__splink__df_predict_examples_per_category",
    # }


def render_splink_comparison_viewer_html(
    comparison_vector_data,
    splink_settings: dict,
    out_path: str,
    overwrite: bool = False,
):

    # When developing the package, it can be easier to point
    # ar the script live on observable using <script src=>
    # rather than bundling the whole thing into the html
    bundle_observable_notebook = True

    template_path = "files/splink_comparison_viewer/template.j2"
    template = pkgutil.get_data(__name__, template_path).decode("utf-8")
    template = Template(template)

    template_data = {
        "comparison_vector_data": json.dumps(
            comparison_vector_data, cls=EverythingEncoder
        ),
        "splink_settings": json.dumps(splink_settings),
    }

    files = {
        "embed": "files/external_js/vega-embed@6.20.2",
        "vega": "files/external_js/vega@5.21.0",
        "vegalite": "files/external_js/vega-lite@5.2.0",
        "svu_text": "files/splink_vis_utils/splink_vis_utils.js",
        "custom_css": "files/splink_comparison_viewer/custom.css",
    }
    for k, v in files.items():
        f = pkgutil.get_data(__name__, v)
        f = f.decode("utf-8")
        template_data[k] = f

    template_data["bundle_observable_notebook"] = bundle_observable_notebook

    rendered = template.render(**template_data)

    if os.path.isfile(out_path) and not overwrite:
        raise ValueError(
            f"The path {out_path} already exists. Please provide a different path."
        )
    else:
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            from pyspark.sql import SparkSession
            from pyspark.dbutils import DBUtils

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
