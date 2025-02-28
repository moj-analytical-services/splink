from __future__ import annotations

import json
from pathlib import Path
import urllib


from splink.internals.charts import ChartReturnType, partial_match_weights_chart
from splink.internals.settings_creator import SettingsCreator


url = "https://raw.githubusercontent.com/moj-analytical-services/splink_demos/master/demo_settings/real_time_settings.json"

with urllib.request.urlopen(url) as u:
    model_json = json.loads(u.read().decode())

def match_weights_chart(settings_dict: Path | str,
                        as_dict: bool = False,
                        sql_dialect_str: str = 'duckdb') -> ChartReturnType:
    """Display a chart of the (partial) match weights of the linkage model

        Args:
            settings_dict (dict | Path | str): A Splink settings dictionary,
                or a path (either as a pathlib.Path object, or a string) to a json file
                defining a settings dictionary for a pre-trained model
            as_dict (bool, optional): If True, return the chart as a dictionary.

        Examples:
            ```py
            from splink.visualisations import match_weights_chart

            altair_chart = match_weights_chart()
            altair_chart.save("mychart.png")
            ```
        Returns:
            altair_chart: An Altair chart
    """

    if not isinstance(settings_dict, SettingsCreator):
            settings_creator = SettingsCreator.from_path_or_dict(settings_dict)
    else:
        settings_creator = settings_dict

    settings = settings_creator.get_settings(sql_dialect_str=sql_dialect_str)
    records = settings._parameters_as_detailed_records

    return partial_match_weights_chart(records, as_dict=as_dict)
