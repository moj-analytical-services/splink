"""Small, dependency-free HTML assembly helpers. Resources are loaded on demand."""

import json
from copy import deepcopy
from decimal import Decimal
from string import Template
from uuid import uuid4

from splink.internals.misc import read_resource

# Keep CDN and packaged runtime versions aligned.
CHART_LIBRARIES = {"vega": "5.31.0", "vega-lite": "5.2.0", "vega-embed": "6.20.2"}


class _HTMLTemplate(Template):
    delimiter = "@@"


def render_html_template(resource, values):
    return _HTMLTemplate(read_resource(resource)).substitute(values)


def json_for_html(value, *, cls=None):
    """Encode script data without allowing an HTML closing script tag."""
    return json.dumps(value, cls=cls).replace("<", r"\u003c")


def load_chart_libraries():
    return {
        name: read_resource(f"internals/files/external_js/{name}@{version}")
        for name, version in CHART_LIBRARIES.items()
    }


class _ChartEncoder(json.JSONEncoder):
    def default(self, value):
        # DuckDB histogram bins can be Decimal, including in a minimal installation.
        if isinstance(value, Decimal):
            return float(value)
        return super().default(value)


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
