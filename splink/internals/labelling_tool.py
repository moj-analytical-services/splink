from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from splink.internals.misc import EverythingEncoder, read_resource
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.vertically_concatenate import compute_df_concat_with_tf

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


def generate_labelling_tool_comparisons(
    linker: "Linker",
    unique_id: str,
    source_dataset: str,
    match_weight_threshold: float = -4,
) -> SplinkDataFrame:
    # ensure the tf table exists
    pipeline = CTEPipeline()
    nodes_with_tf = compute_df_concat_with_tf(linker, pipeline)

    pipeline = CTEPipeline([nodes_with_tf])
    settings = linker._settings_obj

    source_dataset_condition = ""

    if source_dataset is not None:
        sds_col = settings.column_info_settings.source_dataset_column_name
        source_dataset_condition = f"""
          and {sds_col} = '{source_dataset}'
        """

    sql = f"""
    select *
    from __splink__df_concat_with_tf
    where {settings.column_info_settings.unique_id_column_name} = '{unique_id}'
    {source_dataset_condition}
    """

    pipeline.enqueue_sql(sql, "__splink__df_labelling_tool_record")
    splink_df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    matches = linker.inference.find_matches_to_new_records(
        splink_df.physical_name, match_weight_threshold=match_weight_threshold
    )

    return matches


def render_labelling_tool_html(
    linker: "Linker",
    df_comparisons: SplinkDataFrame,
    out_path: str = "labelling_tool.html",
    view_in_jupyter: bool = False,
    show_splink_predictions_in_interface: bool = True,
    overwrite: bool = True,
) -> str:
    from jinja2 import Template

    settings: dict[str, Any] = linker._settings_obj.as_dict()

    logger.warning(
        "\nWARNING:\n"
        "The Splink labelling tool is still in development, which means some "
        "features may change and there may be bugs.\nYour feedback will help us "
        "improve it. Go to\n"
        "github.com/moj-analytical-services/splink/discussions/new?category=general"
        "\nto give us feedback."
    )

    comparisons_recs = df_comparisons.as_pandas_dataframe()

    comparisons_recs = comparisons_recs.replace(r"^\s*$", "", regex=True)

    comparisons_recs = comparisons_recs.fillna(np.nan).replace(
        [np.nan, pd.NA], ["", ""]
    )

    comparisons_recs = comparisons_recs.to_dict(orient="records")
    # Render template with cluster, nodes and edges
    template_path = "internals/files/labelling_tool/template.j2"
    template = Template(read_resource(template_path))

    template_data = {
        "slt": read_resource("internals/files/labelling_tool/slt.js"),
        "d3": read_resource("internals/files/external_js/d3@7.8.5"),
        "stdlib": read_resource("internals/files/external_js/stdlib.js@5.8.3"),
        "pairwise_comparison_data": json.dumps(comparisons_recs, cls=EverythingEncoder),
        "splink_settings_data": json.dumps(settings, cls=EverythingEncoder),
        "view_in_jupyter": view_in_jupyter,
        "show_splink_predictions_in_interface": show_splink_predictions_in_interface,
    }

    rendered = template.render(**template_data)

    if os.path.isfile(out_path) and not overwrite:
        raise ValueError(
            f"The path {out_path} already exists. Please provide a different path."
        )
    else:
        with open(out_path, "w", encoding="utf-8") as html_file:
            html_file.write(rendered)
        # return the rendered dashboard html for inline viewing in the notebook
        return rendered
