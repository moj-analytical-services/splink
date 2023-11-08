import pandas as pd
from splink.duckdb.linker import DuckDBLinker
from IPython.display import display

df = pd.DataFrame(
    [
        {"unique_id": 1, "name": "chris", "test": 1, "test2": 1100, "test3": 0.2},
        {"unique_id": 2, "name": "chris", "test": 1, "test2": 11100, "test3": 0.3},
        {"unique_id": 3, "name": "sam", "test": 2, "test2": 1100, "test3": 0.4},
        {"unique_id": 4, "name": "sam", "test": 2, "test2": 212150, "test3": 0.5},
        {"unique_id": 5, "name": "sam", "test": 3, "test2": 221150, "test3": 0.6},
        {"unique_id": 6, "name": "sam", "test": 4, "test2": 175, "test3": 0.7},
    ]
)

linker = DuckDBLinker(df)

# linker.debug_mode=True

# Classic

charts = linker.profile_columns(
    column_expressions=["test", "test2", "test3"],
    top_n=10,
    bottom_n=5,
)

display(charts)

charts2 = linker.profile_columns(
    column_expressions=["test", "test2", "test3"],
    top_n=10,
    bottom_n=None,
    distribution_plots=False,
)

display(charts2)

charts3 = linker.profile_numeric_columns(
    column_expressions=[
        "test",
        "test2",
        "test3",
    ],
    top_n=10,
    bottom_n=5,
    kde_plots=True,
)

display(charts3)

# charts4=linker.profile_columns(
#     ["test", "test2", "test3"],
#     top_n=None,
#     bottom_n=None,
#     distribution_plots=None,
#     kde_plots=None,
#     correlation_plot=True
#     )

# display(charts4)
