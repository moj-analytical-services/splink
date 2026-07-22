from copy import deepcopy
from typing import Any

# TODO: we can use TypedDict to type this, with NotRequired when we drop python 3.10
default_theme_values = {
    "colour": {
        "positive": "#008000",  # green
        "neutral": "#bbbbbb",
        "negative": "#FF0000",  # red
        "reference_strong": "#000000",  # black
        "text": "#000000",  # black
        "text_soft": "#808080",  # grey
        "text_inverse": "#FFFFFF",  # white
    },
    # vega schemes (https://vega.github.io/vega/docs/schemes/)
    "scheme": {
        "diverging": "redyellowgreen",
    },
}
theme_keys = set(default_theme_values.keys())
theme_sub_keys = {k: set(v.keys()) for k, v in default_theme_values.items()}


class SplinkColourTheme:
    def __init__(self, theme_dict: dict[str, Any] | None = None):
        if theme_dict is None:
            theme_dict = default_theme_values
        self._validate_theme_dict(theme_dict)
        self.theme_dict = deepcopy(theme_dict)

    @staticmethod
    def _validate_theme_dict(theme_dict: dict[str, Any]) -> None:
        provided_theme_elements = set(theme_dict.keys())
        if not provided_theme_elements <= theme_keys:
            raise ValueError(
                f"Can only provide values for theme elements: {theme_keys}.  "
                f"Found extra theme elements: {provided_theme_elements - theme_keys}"
            )

        for theme_element, values in theme_dict.items():
            provided_sub_keys = set(values.keys())
            allowed_sub_keys = theme_sub_keys[theme_element]
            if not provided_sub_keys <= allowed_sub_keys:
                raise ValueError(
                    f"Can only provide values for "
                    f"'{theme_element}': {allowed_sub_keys}.  "
                    f"Found extra values: {provided_sub_keys - allowed_sub_keys}"
                )

    def extend_with(self, theme_dict: dict[str, Any]) -> "SplinkColourTheme":
        self._validate_theme_dict(theme_dict)

        merged_theme = deepcopy(self.theme_dict)
        for item_type, values in theme_dict.items():
            merged_theme[item_type] = {**merged_theme[item_type], **values}

        return SplinkColourTheme(merged_theme)

    def inject_colours_into_spec(self, chart_spec: dict[str, Any]) -> dict[str, Any]:
        for theme_element, value_dict in self.theme_dict.items():
            for name, value in value_dict.items():
                chart_spec = self._replace_colour_placeholder(
                    chart_spec, theme_element, name, value
                )
        return chart_spec

    def _replace_colour_placeholder(
        self,
        chart_spec: dict[str, Any],
        theme_element: str,
        colour_key: str,
        colour_value: str,
    ) -> dict[str, Any]:
        placeholder = f"__splink_{theme_element}_{colour_key}__"
        if isinstance(chart_spec, dict):
            return {
                k: self._replace_colour_placeholder(
                    v, theme_element, colour_key, colour_value
                )
                for k, v in chart_spec.items()
            }
        elif isinstance(chart_spec, list):
            return [
                self._replace_colour_placeholder(
                    item, theme_element, colour_key, colour_value
                )
                for item in chart_spec
            ]
        elif isinstance(chart_spec, str) and chart_spec == placeholder:
            return colour_value
        return chart_spec


default_theme = SplinkColourTheme()

alt = {
    "colour": {
        "positive": "#0571b0",
        "neutral": "#f7f7f7",
        "negative": "#ca0020",
    },
    "scheme": {
        "diverging": "redblue",
    },
}
alt_theme = default_theme.extend_with(alt)

THEME_CATALOGUE = {
    "default": default_theme,
    "alt": alt_theme,
}
