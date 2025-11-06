# Charts in Splink

Interactive charts are a key tool when linking data with Splink. To see all of the charts available, check out the [Splink Charts Gallery](../../charts/index.md).


## How do charts work in Splink?

Charts in Splink are built with [Altair](https://altair-viz.github.io/index.html).

For a given chart, there is usually:

- A template chart definition (e.g. [`match_weights_waterfall.json`](https://github.com/moj-analytical-services/splink/blob/356afa158dc90cbb455f962a9e9ebd1dd51dbc4b/splink/internals/files/chart_defs/match_weights_waterfall.json))
- A function to create the dataset for the chart (e.g. [`records_to_waterfall_data`](https://github.com/moj-analytical-services/splink/blob/356afa158dc90cbb455f962a9e9ebd1dd51dbc4b/splink/internals/waterfall_chart.py#L155-L163))
- A function to read the chart definition, add the data to it, and return the chart itself (e.g. [`waterfall_chart`](https://github.com/moj-analytical-services/splink/blob/356afa158dc90cbb455f962a9e9ebd1dd51dbc4b/splink/internals/charts.py#L190-L205))

??? tip "The Vega-Lite Editor"
    By far the best feature of Vega-Lite is the [online editor](https://vega.github.io/editor) where the JSON schema and the chart are shown side-by-side, showing changes in real time as the editor helps you to navigate the API.

    ![Vega-Lite editor](../../img/charts/Vega-Lite-editor.png)


## Editing existing charts

If you take any Altair chart in HTML format, you should be able to make changes pretty easily with the Vega-Lite Editor.

For example, consider the [`comparator_score_chart`](../../topic_guides/comparisons/choosing_comparators.ipynb#comparing-string-similarity-and-distance-scores) from the [`similarity analysis library`](../../api_docs/exploratory.md#documentation-forsplinkexploratorysimilarity_analysis):

| Before | After |
| ------ | ----- |
| ![Alt text](../../img/charts/old_chart.png) | ![Alt text](../../img/charts/new_chart.png) |

**Desired changes**

- Titles (shared title)
- Axis titles
- Shared y-axis
- Colour scales!! 🤮 (see the Vega [colour schemes](https://vega.github.io/vega/docs/schemes/) docs)
  - red-green is an accessibility no-no
  - shared colour scheme for different metrics
  - unpleasant and unclear to look at
  - legends not necessary (especially when using text labels)
- Text size encoding (larger text for similar strings)
- Remove "_similarity" and "_distance" from column labels
- Fixed column width (rather than chart width)
- Row highlighting (on click/hover)

The old spec can be pasted into the [Vega Lite editor](https://vega.github.io/editor/#/url/vega-lite/N4Igxg9gdgZglgcxALlANzgUwO4tJKAFzigFcJSBnAdTgBNCALFAFgAY2AacaYsiygAlMiRoRQBmDgF9p3RgTABDccgDaoADZKAnpgBOKDSAC2S-QGsUIfZjDjumKJDokkqHpoiGP8TJrprSkhbEG5gpU1MPBA6CDMSIy4ARgBdbn0lKARo9RtMQO4EWycQVLkQQh0AB1yQAEdSLOJCFTg0aIqADxi-AOtIE2rzFW8wypq671coSJAKnV6sfuQQSkJ9N0oAfUIIbcHh0O4q2utpkjnZeREEMUkOE7hCKOthFTNqgAIIGC+AZTgJjg2k2VQBIUwlHG2HoTAebDkoDMlhipzqhEwXQcIAARkpKP4SHVgXQ6K8Kk4XG40VjVKA+oFVsFvNFuDBvGZVCAAHQAJhg43R1kazWebQ6824PV8yyZPCGIz2hhOk3O+hmV24i1l-nl6022R2ewO8SObImZ1WF1mmnmsnSsRUShisxMdTozoAtJQgSDzM9FtctLoDEZkeYrKtbPZxlSIDN3PgIF4fAy5UFIeMIq8PHEElAkpwAKyOzLZXJqEDFTClDIFMoVYWrUVEcXESXdJZ6gZmpVjVVWkA2rUgHXpnvMjZbXb7Q7mC3N4cay5264gRi3e7IKRcSrPXMgd6ET4-P4AETg6yyYEwENZ0O4sIYzGQfJknAjqI8S8x2PG+KEpoxLWKS5KdI4zgJjSP50t2KxrFmg51K2LQShBIAyhOCHzpkypCmq1orraUpjvB+rTkas6moqxyWlMxFXA63Ceq0rpKO61isUoXquNezidNIZZQimkoeDmuTJqm1gkHQmC1FAclEPa3AACTBJuZjWGIhDVJQyAAPQGR0CBKDyCDPIwpC4jycAQAZGmYGYxmYKZXrAZixksDyyQAOw8mwPIAFaUNA4zcYShDQnm3q+sCoKBtYGgADprJRCDGnOfa2KlyCpQASnAYCMOYdCcF8hXFaVqWcKluGjPouWpUF5gQDVqUsjlKDJAFSIdelmU0eaTUgJVJUauV+UAJIAMKCAAgvl57tQqRwNSNLX6G1YQdZCuWBSwEh9WlhoZdRuGYCNY2lZNRXjSt9XKhtrUrZ1l0oIFACcxbHQaM4mhdV13TdXxwPlVUag92XrSgzUvTtiGsvtPLfb9A3ndl715aNwMTRVwN0I1COPd4z1ba9e0fSjP2fv1p2DYDsM4xDZX48VUO0TD2ObdttWI11yCBQAHAAbGj9MY7RWMFbjrOVRYHNrU9TM8xTSNU75R20yd-1ZVLQMs7dYAWDoiv9kT3Pw3zb3IyLQvi7rQ0Lgb42s5extm3hpMq1bu3q4LPJi9rf1UQDmMuyDlW83V0PK5b5MIzbGta8AdOO4z2PXXj+VKFV-ie1zcMJ9blMB75yQO6HevDUzWes-8mLVJuUAF3HRfR-zWMHSnadV07Asy4bbOu633vx-ssJQBYUREyX-s9Yiwfo2H+u17Lk2zQtS2jxb7fbJP08GGrAvd5XZ0rzXmfr8P5g72TE8kIfs9+yfKO+WfDPh2vQ9gyzd8+1tfej8Z7Hy7tTD+ktL6D1dkbEqhN-7jyAVPEBidS5fXfkvCWF9nbfxgcPBBe8D4oLnq-T6FdMHpy-lfIe8sCEgB5kgp+oDkZC3IanHWfcM7QMjkVE2dCGFEKPqg-2gU7YQOwQPZmeD3YK2JrHMehDgFCJIWAoO7CQ7n2rjg6heCo78Naow4hL8wGsPEVoyRddJq5xKvnORnM270IMYI5+ndka+RYGY-u0spEgwbvJZu+jAHOOYVTQ6niuE+OzrLfRYBlCQ2EQLBe4SqHcOzpvRay07FKwUY42J1UElgLYPbChnCUmRLlrjGJcS6AhIDqLZJq8dEg1-iPLJ5tnp5PiSo3KSSSmaK8RHKJVV4FtK9rvXJ1Tam9PUcvcx3jLH4NGYXCZ+TulU2LBgmZWC5mDIqR7JZDiWqdJqQU5GEhilbMoY01Jey+EHJyUcyZpyqYSB7hw-pESFkyKqas4xyM+R8gaVA8pRsO4k3GY835riXkXN7h8spCyc5500D8rpfzQkeL6Z-a5IKASNwCfciFucnlrIDkUoS4VvR8VaAJJKlzSk4sRdEwlI0ogdCgJQMQIgW7PLJUC7RNyN5zQyXQtlThOWYhILUkWvUsWQIFbirOorMDsoldyqZsr6XwsZdfFpULwWspVeKrlUreV8k1XC7FwKmXDJcQapmYqOUmp5aS81i8tVWoVTaxgyrVXOtqRIC17zPUWOvrQllDqjVOslS69FyBvLustfK0NNDeGmwjdjR1arTWkoTfylN0jeG+uNTG2pxYg0aJDfMsNRUwXyPGVm-1vK81yokdWmh1ilC2L5vazNUbs2xuhTuCtsyBm4N8fipwxbo3qt5f5RNwbk3tt0cynt9aRqendJkUg2xG2lt5YFBdlal27KFVvTJa77E5M3QYJQO692ztJTKo9o7Pk1ohnQm927d39qbaS6ZSa22ntBuDVpl7snjK-Xen9fr93-pHdssdTShlwLteupmUH72-rg3GgDi6gPjpQ5+jit6sOwcfXGwNL7ENvtTfs8D7SMMke-Q+nNcaW0epPYR256aGNjI3cx6DrHB1J3jQhq51rr7fIzalTDMGS0UaHeW6jEmvXvrrVeyDgmyMKbY0OjjgGdncasci4jW6hPYcU6Jqj+bl0Tv8VOmTTpzM6ZnXp0T86KWyCAA) and edited as shown in the video below:

![type:video](../../img/charts/charts.mp4)


Check out the final, improved version [chart specification.](https://vega.github.io/editor/#/url/vega-lite/N4IgLglmA2CmIC5RlgDzIkAJWBDMAtrgA4DOABAPYBm5pYAThAHYDm5AxpQcbk6ZWbkCsRhA6kQAGhC5mHABaUGmAhAAm6uNJDVBYAMoQAXvAQBGAGwBfGQq7z8iANqhouAJ6wVCVyCIMANaYDLAcGDKw8pTqLKyIoFzQygm6ELDQ6pikXKE6ObjaSCDq3LgsLgAMUuYAujI5CrAimKyhUQBG0ACu8LbgHsRmIACO3XKQYPgQAG7wMnCsUVkIzN3Q0P2oqdTpmZhcPHz4KTJgg8PKscyFOpNFaxv9Hjt7KyD0TGykAPpglD9Drw8mcLpgrixbmcoEUQAYxGxONxgRABMwdLhUKjUvdYAAxfRGUyIcwAFmsFOhMGGRjU7iY5x0AHcNGAFKl6LBiIhSZV+k0IKwFBhipzuQgAMx82ygALBYrnIaYFDoHQdXCkDIsYZqTTafpRLjXeKikxmUC7DLvHLKeYfDiFc0gBhyJYuAAcNVJtX6i2WiEemzOaBFFre2Vydr0DCIIpAADoAEzUO5ghCjcbMSbTOYgLavK0HZHHf4qUFK9MQm7QDFYyTFdwdDIErNEszmROUkAvYqW-bpz5xX7-QHFkEDCsgKu3CnWeolfC4VI3Frp9SLgC0pAgdL4UBelLcnm8LllfHlzrCERAhpicVSSRSvfDA8j+QdRVAjWawy8G0oTLKK63iwFkMihHMDCaogjC9P0irDGMExQDmdp+swKyBvmz6FumQIlqcE6XAw1xQuAMJmIGMiYtiDa4E20AtoYZokp2zwFv2HwIqww4Avh44IeCJGQjW1F1gG6ybPBFGYAAIqiUzyHaLLqGyHIoOKvL8rAgrCupXKIFKh7+OeOJpuAIZqhqWrMDqGhaH0kTRMaHIsThnE2uOBSfs6wEel684Qd40EILBsC+rASwYRJTzBqq7nWm+5aIZm2aQLm2FhrhID8ScZZEUJpE1uxCXZNxvGjkcAnmdOxUUvO65TMuuCrguUwbrE9ByBwfRzuBsACNAuaih+TqPj4IAsOoXLLFE149h8Ch8KB+RuZNGEzRtWZ5v0AAk35EJgwpgGQCAAPRnXMrC4PGrBQAo3QdPGECUGdB24JdkW4Bu0BQLAl2kvG5gAOzxpU8YAFZojojXWWA9agLDW47hA9L7pgrgADpcV8PF-HxY6wNjCDYwASuIS0kVI5Dk4ofDqNjUjY7lpbE9jEN8JQjPY55RMkmDMo8+V+OVcCfMkyAtOU+o1OkwAkgAwlgACCpOydzOVjnlbMgBzDBc9IPORsT4OkhKgs40OIv8eLZMU-Tsv2wwGss8oOt6wbTP2raJvxgAnAArBbg7fNbhM61LDvkBApN0yRLta6ziDs5zGu877gfB8LI42xHTsyzT9vqM7hua1V2vJ7rqel+niDg5nUjAELuMVbnleR1TheKAn5dJxLHtp8bdfxu6NiN83Vs5+H7f547HCBD3wIV-31de7XCDg8D5vj5bodT1VtuS7PXeBB4i8ESXK-64PPvD5Y7pZy3YcH3nccF-J8-ny6fcp9fNdDxveMY8m67zxvvMWr9pZz09szRObtK4D3-rfQBW9H6TwJi-Geb9Za4DphkL+y9f4wO9qEX2wNzBoL3hgiBWCoHkHhFyJozACE-yrn-NeADTbbxASHMB1DlqQKjh3BmpdXaXyIT8FkzBAhwBLhw5B5gBY714a3aeEthGy0VirNWLD4FXwBFImR3gb6kOHmbShfDRYCNoUIp2ujxFsIMSwIxcijbIPrsDCxqjMHqOPjHN+9j3ac0kc42RJjxb1yDso7O-DTG+OwV3JaxdAkIOCYYsJSC4keK8c-Gh8S6FSxSfokJ0iMnyKyf7Ch0Sn7gOsfk2xn9RFwIcR7EpLjwm+3dFUnhMSrFxLtgk2mp8ikSPScYzJESgEP2qegvph8NHkA-gvJpvc9GjNCeM8pkzgETyoXMwRndabELEUE-WbSyluIqV0nJtT+lH0Gbgpa+CVlL1Ya0sZriSGTOBuSGZey271M7gw4gTCRmOPOZsy5kzzF-MsQCgZBT85go5hwB08cJnE0UXyWF3i8kIqEVo1W6sXkXyCai+mHTh6VGmT0mpsT5nHw7si3BaKRFbM6Ts0BuK6n4s7v46WzLyXovZfzbFtLZnwvuYiumySSXfzWVXIVbKoWYqUeK-5ajeUF0KXKwhirWWUsAQHTxOLck8qlQ05ZXsTmpKVYa8GEoaW7LhZqi1hzxDDN1W8llFKMXDwlNw513K7kLKWYKg1frAGJk7Ka25DLBniGOc0slEaRWAMdTc+lBztWPNwM861ybbWppVWY356qXU+K1dTYFoKvUKpRcWr5vtqVzhhpuTqikeoY3LcG+N0qBV1ocXAOYzBSDCh0swyN4MxVBrNSGxlhKdGDp1sOqIY6UAsENZYNVs643ZrnpTMFq7R3js3ZGrFmb9k2L5bHAdBbVlDtgCO9dE7DWJh3VyudfaGlJM+TaiWx6X1nrTe+mdn693Xu1RTI9T612nsnWmiUH6VFfv3SfGDz74OGsBmBlDEHAVQfnmfZdldANYcjThy9krQ0eow3BjdCGS0IADsh3p1HGWJroyehj2HWN0qvQRnBeDoBcaA4xpthk+MStdQsmtURRPkbTaDXDbGZMcYCSRiW64RAum6D8MjPGp1SY1ZWt12rF3Evva8hV2nvC4D0wZ19kbt0qf4+xhNccwW2d0-p2D3GnNpovbGrNkHqb8t9VZ0lldvP2d85hwzgXjMVrxWZg9uBZWRflQ4mLDm-NicNUFntqHQuJK8y1OzuX4sBaY0h1z0nTMLKGWVnTsXHPAaY5R4LAmq0n2I5lvVOW4v0eqxJhAnWiv4Z62GzT2NBttfE+vFjdWTMpca5xmbC4WuVeG+10b43d0hcEzTXN+bYEPp1nNvLimatJd7WhuT4n-2zfKz5+bhrlOtopEAA)

??? info "Before-After diff"

    ```diff
    @@ -1,9 +1,8 @@
    {
    -  "config": {
    -    "view": {
    -      "continuousWidth": 400,
    -      "continuousHeight": 300
    -    }
    +  "title": {
    +    "text": "Heatmaps of string comparison metrics",
    +    "anchor": "middle",
    +    "fontSize": 16
      },
      "hconcat": [
        {
    @@ -18,25 +17,32 @@
                      0,
                      1
                    ],
    -                "range": [
    -                  "red",
    -                  "green"
    -                ]
    +                "scheme": "greenblue"
                  },
    -              "type": "quantitative"
    +              "type": "quantitative",
    +              "legend": null
                },
                "x": {
                  "field": "comparator",
    -              "type": "ordinal"
    +              "type": "ordinal",
    +              "title": null
                },
                "y": {
                  "field": "strings_to_compare",
    -              "type": "ordinal"
    +              "type": "ordinal",
    +              "title": "String comparison",
    +              "axis": {
    +                "titleFontSize": 14
    +              }
                }
              },
    -          "height": 300,
    -          "title": "Heatmap of Similarity Scores",
    -          "width": 300
    +          "title": "Similarity",
    +          "width": {
    +            "step": 40
    +          },
    +          "height": {
    +            "step": 30
    +          }
            },
            {
              "mark": {
    @@ -44,6 +50,16 @@
                "baseline": "middle"
              },
              "encoding": {
    +            "size": {
    +              "field": "score",
    +              "scale": {
    +                "range": [
    +                  8,
    +                  14
    +                ]
    +              },
    +              "legend": null
    +            },
                "text": {
                  "field": "score",
                  "format": ".2f",
    @@ -51,7 +67,10 @@
                },
                "x": {
                  "field": "comparator",
    -              "type": "ordinal"
    +              "type": "ordinal",
    +              "axis": {
    +                "labelFontSize": 12
    +              }
                },
                "y": {
                  "field": "strings_to_compare",
    @@ -72,29 +91,33 @@
                "color": {
                  "field": "score",
                  "scale": {
    -                "domain": [
    -                  0,
    -                  5
    -                ],
    -                "range": [
    -                  "green",
    -                  "red"
    -                ]
    +                "scheme": "yelloworangered",
    +                "reverse": true
                  },
    -              "type": "quantitative"
    +              "type": "quantitative",
    +              "legend": null
                },
                "x": {
                  "field": "comparator",
    -              "type": "ordinal"
    +              "type": "ordinal",
    +              "title": null,
    +              "axis": {
    +                "labelFontSize": 12
    +              }
                },
                "y": {
                  "field": "strings_to_compare",
    -              "type": "ordinal"
    +              "type": "ordinal",
    +              "axis": null
                }
              },
    -          "height": 300,
    -          "title": "Heatmap of Distance Scores",
    -          "width": 200
    +          "title": "Distance",
    +          "width": {
    +            "step": 40
    +          },
    +          "height": {
    +            "step": 30
    +          }
            },
            {
              "mark": {
    @@ -102,6 +125,17 @@
                "baseline": "middle"
              },
              "encoding": {
    +            "size": {
    +              "field": "score",
    +              "scale": {
    +                "range": [
    +                  8,
    +                  14
    +                ],
    +                "reverse": true
    +              },
    +              "legend": null
    +            },
                "text": {
                  "field": "score",
                  "type": "quantitative"
    @@ -124,7 +158,9 @@
      ],
      "resolve": {
        "scale": {
    -      "color": "independent"
    +      "color": "independent",
    +      "y": "shared",
    +      "size": "independent"
        }
      },
      "$schema": "https://vega.github.io/schema/vega-lite/v4.17.0.json",
    ```
