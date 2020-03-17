pi_iteration_chart_def = {
    "config": {
        "view": {"width": 400, "height": 300},  # pragma: no cover
        "mark": {"tooltip": None},
        "title": {"anchor": "middle"},
    },
    "hconcat": [
        {
            "mark": "bar",
            "encoding": {
                "color": {"type": "nominal", "field": "value"},
                "row": {
                    "type": "nominal",
                    "field": "column",
                    "sort": {"field": "gamma"},
                },
                "tooltip": [
                    {"type": "quantitative", "field": "probability"},
                    {"type": "ordinal", "field": "iteration"},
                    {"type": "nominal", "field": "column"},
                    {"type": "nominal", "field": "value"},
                ],
                "x": {"type": "ordinal", "field": "iteration"},
                "y": {
                    "type": "quantitative",
                    "aggregate": "sum",
                    "axis": {"title": "𝛾 value"},
                    "field": "probability",
                },
            },
            "height": 100,
            "resolve": {"scale": {"y": "independent"}},
            "title": "Non Match",
            "transform": [{"filter": "(datum.match === 0)"}],
        },
        {
            "mark": "bar",
            "encoding": {
                "color": {"type": "nominal", "field": "value"},
                "row": {
                    "type": "nominal",
                    "field": "column",
                    "sort": {"field": "gamma"},
                },
                "tooltip": [
                    {"type": "quantitative", "field": "probability"},
                    {"type": "ordinal", "field": "iteration"},
                    {"type": "nominal", "field": "column"},
                    {"type": "nominal", "field": "value"},
                ],
                "x": {"type": "ordinal", "field": "iteration"},
                "y": {
                    "type": "quantitative",
                    "aggregate": "sum",
                    "axis": {"title": "𝛾 value"},
                    "field": "probability",
                },
            },
            "height": 100,
            "resolve": {"scale": {"y": "independent"}},
            "title": "Match",
            "transform": [{"filter": "(datum.match === 1)"}],
        },
    ],
    "data": {"values": None},
    "title": "Probability distribution of comparison vector values by iteration number",
    "$schema": "https://vega.github.io/schema/vega-lite/v3.4.0.json",
}


lambda_iteration_chart_def = {
    "config": {
        "view": {"width": 400, "height": 300},
        "mark": {"tooltip": None},
    },  # pragma: no cover
    "data": {"values": None},
    "mark": "bar",
    "encoding": {
        "x": {"type": "ordinal", "field": "iteration"},
        "y": {
            "type": "quantitative",
            "axis": {"title": "λ (estimated proportion of matches)"},
            "field": "λ",
        },
        "tooltip": [
            {"type": "quantitative", "field": "λ"},
            {"type": "ordinal", "field": "iteration"},
        ],
    },
    "title": "Lambda value by iteration",
    "$schema": "https://vega.github.io/schema/vega-lite/v3.4.0.json",
}


ll_iteration_chart_def = {
    "config": {
        "view": {"width": 400, "height": 300},
        "mark": {"tooltip": None},
    },  # pragma: no cover
    "data": {"values": None},
    "mark": "bar",
    "encoding": {
        "x": {"type": "ordinal", "field": "iteration"},
        "y": {
            "type": "quantitative",
            "axis": {"title": "Log likelihood by iteration number"},
            "field": "log_likelihood",
        },
        "tooltip": [
            {"type": "quantitative", "field": "log_likelihood"},
            {"type": "ordinal", "field": "iteration"},
        ],
    },
    "title": "Log likelihood value by iteration",
    "$schema": "https://vega.github.io/schema/vega-lite/v3.4.0.json",
}


probability_distribution_chart = {
    "config": {
        "view": {"width": 400, "height": 300},  # pragma: no cover
        "mark": {"tooltip": None},
        "title": {"anchor": "middle"},
    },
    "hconcat": [
        {
            "mark": "bar",
            "encoding": {
                "color": {"type": "nominal", "field": "match"},
                "row": {
                    "type": "nominal",
                    "field": "column",
                    "sort": {"field": "gamma"},
                },
                "tooltip": [
                    {"type": "nominal", "field": "column"},
                    {"type": "quantitative", "field": "probability", "format": ".4f"},
                    {"type": "ordinal", "field": "value"},
                ],
                "x": {"type": "quantitative", "field": "probability"},
                "y": {
                    "type": "nominal",
                    "axis": {"title": "𝛾 value"},
                    "field": "value",
                },
            },
            "resolve": {"scale": {"y": "independent"}},
            "transform": [{"filter": "(datum.match === 0)"}],
            "width": 150,
            "height": 50
        },
        {
            "mark": "bar",
            "encoding": {
                "color": {"type": "nominal", "field": "match"},
                "row": {
                    "type": "nominal",
                    "field": "column",
                    "sort": {"field": "gamma"},
                },
                "tooltip": [
                    {"type": "nominal", "field": "column"},
                    {"type": "quantitative", "field": "probability", "format": ".4f"},
                    {"type": "ordinal", "field": "value"},
                ],
                "x": {"type": "quantitative", "field": "probability"},
                "y": {
                    "type": "nominal",
                    "axis": {"title": "𝛾 value"},
                    "field": "value",
                },
            },
            "resolve": {"scale": {"y": "independent"}},
            "transform": [{"filter": "(datum.match === 1)"}],
            "width": 150,
            "height": 50
        },
    ],
    "data": {"values": None},
    "title": "Probability distribution of comparison vector values, m=0 and m=1",
    "$schema": "https://vega.github.io/schema/vega-lite/v3.4.0.json",
}

adjustment_weight_chart_def = {
    "config": {
        "view": {"width": 400, "height": 300},
        "mark": {"tooltip": None},
        "title": {"anchor": "middle"},
    },
    "data": {"values": None},
    "mark": "bar",
    "encoding": {
        "color": {
            "type": "quantitative",
            "field": "normalised_adjustment",
            "scale": {
                "domain": [-0.5, -0.4, 0, 0.4, 0.5],
                "range": ["red", "orange", "green", "orange", "red"],
            },
        },
        "row": {"type": "nominal", "field": "col_name", "sort": {"field": "gamma"}},
        "tooltip": [
            {"type": "nominal", "field": "col_name"},
            {"type": "quantitative", "field": "normalised_adjustment"},
        ],
        "x": {
            "type": "quantitative",
            "axis": {"title": "Influence on match probabiity."},
            "field": "normalised_adjustment",
            "scale": {"domain": [-0.5, 0.5]},
        },
        "y": {"type": "nominal", "field": "level"},
    },
    "height": 50,
    "resolve": {"scale": {"y": "independent"}},
    "title": "Influence of comparison vector values on match probability",
    "$schema": "https://vega.github.io/schema/vega-lite/v3.4.0.json",
}

adjustment_factor_chart_def = {
    "config": {"view": {"width": 400, "height": 300}, "mark": {"tooltip": None}},
    "data": {"values": None},
    "mark": "bar",
    "encoding": {
        "color": {
            "type": "quantitative",
            "field": "normalised",
            "scale": {
                "domain": [-0.5, -0.4, 0, 0.4, 0.5],
                "range": ["red", "orange", "green", "orange", "red"],
            },
        },
        "tooltip": [
            {"type": "nominal", "field": "field"},
            {"type": "quantitative", "field": "normalised"},
        ],
        "x": {
            "type": "quantitative",
            "field": "normalised",
            "scale": {"domain": [-0.5, 0.5]},
        },
        "y": {"type": "nominal", "field": "col_name", "sort": {"field": "gamma"}},
    },
    "$schema": "https://vega.github.io/schema/vega-lite/v3.4.0.json",
}


multi_chart_template = """
<!DOCTYPE html>
<html>
<head>
  <script src="https://cdn.jsdelivr.net/npm/vega@{vega_version}"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-lite@{vegalite_version}"></script>
  <script src="https://cdn.jsdelivr.net/npm/vega-embed@{vegaembed_version}"></script>
</head>
<body>

<div id="vis1"></div><div id="vis2"></div>
<br/>
<div id="vis3"></div><div id="vis5"></div>
<br/>
<div id="vis4"></div>




<script type="text/javascript">
  vegaEmbed('#vis1', {spec1}).catch(console.error);
  vegaEmbed('#vis2', {spec2}).catch(console.error);
  vegaEmbed('#vis3', {spec3}).catch(console.error);
  vegaEmbed('#vis4', {spec4}).catch(console.error);
  vegaEmbed('#vis5', {spec5}).catch(console.error);
</script>
</body>
</html>
"""  # pragma: no cover

