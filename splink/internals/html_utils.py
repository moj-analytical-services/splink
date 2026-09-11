"""Small, dependency-free HTML assembly helpers. Resources are loaded on demand."""

import json
from copy import deepcopy
from decimal import Decimal
from string import Template
from uuid import uuid4

from splink.internals.misc import read_resource

# Keep CDN and packaged runtime versions aligned.
CHART_LIBRARIES = {"vega": "5.31.0", "vega-lite": "5.2.0", "vega-embed": "6.20.2"}
CHART_LIBRARY_INTEGRITIES = {
    "vega": "sha384-Aetxbwjx5EnbMsEyBnb1wt8zDBbF6YNdfK+1wmv97rUqFETerVNhJhJ3zYshzN9J",
    "vega-lite": (
        "sha384-tU6fj0fI2gxrcWwC7uBMp70QvipC9ukjcXyOs85VMmdCq33CrA7xQ3nJkJu0SmDm"
    ),
    "vega-embed": (
        "sha384-oP1rwLY7weRZ5jvAVzfnJsAn+sYA69rQC4geH82Y9oMvr8ruA1oeE9Jkft2noCHR"
    ),
}


class _HTMLTemplate(Template):
    delimiter = "@@"


def render_html_template(resource, values):
    return _HTMLTemplate(read_resource(resource)).substitute(values)


def json_for_html(value, *, cls=None):
    """Encode data for a script element without permitting HTML delimiters."""
    return (
        json.dumps(value, cls=cls)
        .replace("<", r"\u003c")
        .replace(">", r"\u003e")
        .replace("&", r"\u0026")
    )


def load_chart_libraries():
    return {
        name: read_resource(f"internals/files/external_js/{name}@{version}")
        for name, version in CHART_LIBRARIES.items()
    }


class _ChartEncoder(json.JSONEncoder):
    def default(self, o):
        # DuckDB histogram bins can be Decimal, including in a minimal installation.
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


def chart_html(spec, *, inline=False, fullhtml=False):
    spec = deepcopy(spec)
    view = spec.setdefault("config", {}).setdefault("view", {})
    view.setdefault("continuousWidth", 300)
    view.setdefault("continuousHeight", 300)
    values = {
        "output_div": "splink-chart-" + uuid4().hex,
        "spec": json_for_html(spec, cls=_ChartEncoder),
        "paths": json_for_html(
            {
                name: f"https://cdn.jsdelivr.net/npm/{name}@{version}?noext"
                for name, version in CHART_LIBRARIES.items()
            }
        ),
        "integrities": json_for_html(CHART_LIBRARY_INTEGRITIES),
    }
    if inline:
        libraries = load_chart_libraries()
        values.update({name.replace("-", "_"): src for name, src in libraries.items()})
    fragment = render_html_template(
        "internals/files/templates/"
        + ("single_chart_template.html" if inline else "chart_fragment.html"),
        values,
    )
    if fullhtml:
        return (
            '<!DOCTYPE html>\n<html lang="en"><head><meta charset="utf-8">'
            "<title>Splink chart</title></head><body>" + fragment + "</body></html>"
        )
    return fragment
