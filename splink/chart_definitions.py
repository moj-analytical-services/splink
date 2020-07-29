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
            "height": 150,
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
            "height": 150,
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
                "color": {
                    "type": "nominal",
                    "field": "match",
                    "scale": {"domain": [0, 1], "range": ["red", "green"]}
                },
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
                "color": {
                    "type": "nominal",
                    "field": "match",
                    "scale": {"domain": [0, 1], "range": ["red", "green"]}
                },
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

gamma_distribution_chart_def = {
    "config": {
        "view": {"width": 400, "height": 300},  # pragma: no cover
        "mark": {"tooltip": None},
        "title": {"anchor": "middle"},
    },
            "mark": "bar",
            "encoding": {
                "row": {
                    "type": "nominal",
                    "field": "column",
                    "sort": {"field": "column"},
                },
                "tooltip": [
                    {"type": "nominal", "field": "column"},
                    {"type": "quantitative", "field": "level_proportion", "format": ".4f"},
                    {"type": "ordinal", "field": "level"},
                ],
                "x": {"type": "quantitative", "field": "level_proportion"},
                "y": {
                    "type": "nominal",
                    "axis": {"title": "𝛾 value"},
                    "field": "level",
                },
            },
            "resolve": {"scale": {"y": "independent"}},
            "width": 150,
            "height": 50,
    "data": {"values": None},
    "title": "Distribution of (non-null) 𝛾 values",
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
        "row": {"type": "nominal", "field": "column", "sort": {"field": "column"}},
        "tooltip": [
            {"type": "nominal", "field": "column"},
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
        "y": {"type": "nominal", "field": "column", "sort": {"field": "gamma"}},
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

<div id="vis1"></div><div id="vis2"></div><div id="vis3"></div>
<br/>
<div id="vis4"></div><div id="vis5"></div>
<br/>
<div id="vis6"></div>
<br/>
<div id="vis7"></div>



<script type="text/javascript">
  vegaEmbed('#vis1', {spec1}).catch(console.error);
  vegaEmbed('#vis2', {spec2}).catch(console.error);
  vegaEmbed('#vis3', {spec3}).catch(console.error);
  vegaEmbed('#vis4', {spec4}).catch(console.error);
  vegaEmbed('#vis5', {spec5}).catch(console.error);
  vegaEmbed('#vis6', {spec6}).catch(console.error);
  vegaEmbed('#vis7', {spec7}).catch(console.error);
</script>
</body>
</html>
"""  # pragma: no cover

adjustment_history_chart_def = {
    'hconcat': [{
        'mark': 'bar',
         'encoding': {
             'color': {
                 'type': 'ordinal',
                 'field': 'level',
                 'legend': {},
                 'scale': {
                     'scheme': 'redyellowgreen'}
             },
             'tooltip': [
                 {'type': 'nominal', 'field': 'column'},
                 {'type': 'ordinal', 'field': 'level'},
                 {'type': 'quantitative', 'field': 'm'},
                 {'type': 'quantitative', 'field': 'u'},
                 {'type': 'quantitative', 'field': 'normalised_adjustment'}],
             'x': {
                 'type': 'ordinal', 
                 'field': 'level'
             },
             'y': {
                 'type': 'quantitative',
                 'axis': {'title': 'Influence on match probability'},
                 'field': 'normalised_adjustment',
                 'scale': {'domain': [-0.5, 0.5]}
             }
         },
        "transform": [
            {"filter": "(datum.final === true)"}
        ],
        'width': 100,
        'height': 150,
        'selection': {
            'selector190': {'type': 'single', 'on': 'mouseover', 'fields': ['level', 'column']}
        }
    },
        {
            'layer': [{
                'mark': 'line',
                'height': 150,
                'encoding': {
                    'color': {
                        'type': 'nominal',
                        'field': 'level',
                        'legend': {},
                        'scale': {'scheme': 'redyellowgreen'}
                    },
                    'opacity': {
                        'condition': {
                            'value': 0.8,
                            'selection': {'not': 'selector190'}
                        },
                        'value': 1 
                    },
                    'size': {
                        'condition': {
                            'value': 3, 
                            'selection': {'not': 'selector190'}
                        },
                        'value': 5
                    },
                    'tooltip': [
                        {'type': 'nominal', 'field': 'column'},
                        {'type': 'quantitative', 'field': 'iteration'},
                        {'type': 'ordinal', 'field': 'level'},
                        {'type': 'quantitative', 'field': 'm'},
                        {'type': 'quantitative', 'field': 'u'},
                        {'type': 'quantitative', 'field': 'normalised_adjustment'}
                    ],
                    'x': {
                        'type': 'ordinal',     
                        'axis': {'title': 'Iteration'},
                        'field': 'iteration'
                    },
                    'y': {
                        'type': 'quantitative',
                        'axis': {'title': 'Influence on match probability'},
                        'field': 'normalised_adjustment',
                        'scale': {'domain': [-0.5, 0.5]}
                    }
                }
            }
            ]
        }
    ],
    'title': {'text': None, 'orient': 'top', 'dx': 200},
    'data': {'values': None}
}