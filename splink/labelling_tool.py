import json
import os
import pkgutil

import numpy as np
import pandas as pd
from jinja2 import Template

from .misc import EverythingEncoder
from .splink_dataframe import SplinkDataFrame


def render_labelling_tool_html(
    settings: dict,
    df_comparisons: SplinkDataFrame,
    out_path="labelling_tool.html",
    overwrite: bool = True,
):

    comparisons_recs = df_comparisons.as_pandas_dataframe()

    # deal with col a
    comparisons_recs = comparisons_recs.replace(r"^\s*$", "", regex=True)

    # deal with col b and c
    comparisons_recs = comparisons_recs.fillna(np.nan).replace(
        [np.nan, pd.NA], ["", ""]
    )

    comparisons_recs = comparisons_recs.to_dict(orient="records")
    # Render template with cluster, nodes and edges
    template_path = "files/labelling_tool/template.j2"
    template = pkgutil.get_data(__name__, template_path).decode("utf-8")
    template = Template(template)

    slt_text = pkgutil.get_data(__name__, "files/labelling_tool/slt.js")
    slt_text = slt_text.decode("utf-8")

    custom_css = pkgutil.get_data(__name__, "files/labelling_tool/custom_css.css")
    custom_css = custom_css.decode("utf-8")

    template_data = {
        "slt": slt_text,
        "custom_css": custom_css,
        "pairwise_comparison_data": json.dumps(comparisons_recs, cls=EverythingEncoder),
        "splink_settings_data": json.dumps(settings, cls=EverythingEncoder),
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
