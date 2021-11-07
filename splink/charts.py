import pkgutil
import json
import os

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


iframe_message = """
To view in Jupyter you can use the following command:

from IPython.display import IFrame
IFrame(src="./{filename}", width=1000, height=500)
"""


def save_offline_chart(
    altair_chart, filename="my_chart.html", overwrite=False, print_msg=True
):

    if os.path.isfile(filename) and not overwrite:
        raise ValueError(
            f"The path {filename} already exists. Please provide a different path."
        )

    # get altair chart as json
    path = "files/templates/single_chart_template.txt"
    template = pkgutil.get_data(__name__, path).decode("utf-8")

    fmt_dict = _load_external_libs()

    fmt_dict["mychart"] = _make_json(altair_chart)

    with open(filename, "w") as f:
        f.write(template.format(**fmt_dict))

    if print_msg:
        print(f"Chart saved to {filename}")
        print(iframe_message.format(filename=filename))
