import pkgutil
import json

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False


def load_chart_definition(filename):
    path = f"files/chart_defs/{filename}"
    data = pkgutil.get_data(__name__, path)
    schema = json.loads(data)
    return schema


def _load_multi_chart_template():
    path = "files/templates/multi_chart_template.txt"
    template = pkgutil.get_data(__name__, path).decode("utf-8")
    return template


def _load_external_libs():

    to_load = {
        "vega-embed": "files/external_js/vega-embed@6.14.2",
        "vega-lite": "files/external_js/vega-lite@4.17.0",
        "vega": "files/external_js/vega@5.17.1",
    }

    loaded = {}
    for k, v in to_load.items():
        script = pkgutil.get_data(__name__, v).decode("utf-8")
        loaded[k] = script

    return loaded


def altair_if_installed_else_json(chart_dict):
    if altair_installed:
        return alt.Chart.from_dict(chart_dict)
    else:
        return chart_dict


def _make_json(chart_or_dict):
    if altair_installed:
        return chart_or_dict.to_json(indent=None)
    else:
        return json.dumps(chart_or_dict)
