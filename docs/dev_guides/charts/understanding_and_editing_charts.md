# Charts in Splink

Interactive charts are a key tool when linking data with Splink. To see the
available charts, visit the [Splink Charts Gallery](../../charts/index.md).

## How do charts work in Splink?

Splink charts are Vega-Lite specifications. A `SplinkChart` combines a packaged
template with records and exposes the completed specification through
`chart.chart_dict`.

Basic chart display and HTML export do not require Altair. When Altair is
installed, use `chart.altair_chart` to customise a Splink chart with Altair or
use Altair's advanced export formats.

For a given chart, there is usually:

- A template chart definition, such as
  [`match_weights_waterfall.json`](https://github.com/moj-analytical-services/splink/blob/master/splink/internals/files/chart_defs/match_weights_waterfall.json).
- A function that prepares its dataset, such as
  [`records_to_waterfall_data`](https://github.com/moj-analytical-services/splink/blob/master/splink/internals/waterfall_chart.py).
- A `SplinkChart` subclass that adds its records and applies chart-specific
  changes to the specification.

??? tip "The Vega-Lite Editor"
    Use `chart.chart_dict` as the input to the [Vega-Lite editor](https://vega.github.io/editor) to inspect and edit a Splink-produced chart specification.

## Editing existing charts

Start with the `chart_dict` of the Splink chart you want to edit. Copy the
dictionary into the Vega-Lite editor, make the change, and move the resulting
specification change into the chart template or its `SplinkChart` subclass.

Altair can still be useful for prototyping in the documentation environment.
That is separate from Splink's chart contract: a Splink chart is a Vega-Lite
specification first, and Altair conversion is explicit and optional.
