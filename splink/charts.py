import pkg_resources
import json

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False


def load_chart_definition(filename):
    path = f"files/chart_defs/{filename}"
    with pkg_resources.resource_stream(__name__, path) as io:
        schema = json.load(io)
    return schema


def altair_if_installed_else_json(chart_dict):
    if altair_installed:
        return alt.Chart.from_dict(chart_dict)
    else:
        return chart_dict
