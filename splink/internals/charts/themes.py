from typing import Any

# TODO: we can use TypedDict to type this, with NotRequired when we drop python 3.10
default_colours = {
    "positive": "#008000",  # green
    "negative": "#FF0000",  # red
    "reference_strong": "#000000",  # black
    "text": "#000000",  # black
    "text_soft": "#808080",  # grey
    "text_inverse": "#FFFFFF",  # white
}
colour_keys = set(default_colours.keys())


class SplinkColourTheme:
    def __init__(self, theme_dict: dict[str, Any] | None = None):
        if theme_dict is None:
            theme_dict = default_colours
        self._validate_colour_dict(theme_dict)
        # make a copy so we don't need to worry about mutation
        self.theme_dict = {**theme_dict}

    @staticmethod
    def _validate_colour_dict(theme_dict: dict[str, Any]) -> None:
        provided_colours = set(theme_dict.keys())
        if not provided_colours <= colour_keys:
            raise ValueError(
                f"Can only provide values for colours: {colour_keys}.  "
                f"Found extra colours: {provided_colours - colour_keys}"
            )

    def extend_with(self, theme_dict: dict[str, Any]) -> "SplinkColourTheme":
        self._validate_colour_dict(theme_dict)
        return SplinkColourTheme({**self.theme_dict, **theme_dict})

    def inject_colours_into_spec(self, chart_spec: dict[str, Any]) -> dict[str, Any]:
        for key, value in self.theme_dict.items():
            chart_spec = self._replace_colour_placeholder(chart_spec, key, value)
        return chart_spec

    def _replace_colour_placeholder(
        self, chart_spec: dict[str, Any], colour_key: str, colour_value: str
    ) -> dict[str, Any]:
        placeholder = f"__splink_colour_{colour_key}__"
        if isinstance(chart_spec, dict):
            return {
                k: self._replace_colour_placeholder(v, colour_key, colour_value)
                for k, v in chart_spec.items()
            }
        elif isinstance(chart_spec, list):
            return [
                self._replace_colour_placeholder(item, colour_key, colour_value)
                for item in chart_spec
            ]
        elif isinstance(chart_spec, str) and chart_spec == placeholder:
            return colour_value
        return chart_spec


default_theme = SplinkColourTheme()

alt = {
    "positive": "#0571b0",
    "negative": "#ca0020",
}
alt_theme = default_theme.extend_with(alt)

THEME_CATALOGUE = {
    "default": default_theme,
    "alt": alt_theme,
}
