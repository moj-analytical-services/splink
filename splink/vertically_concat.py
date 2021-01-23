from functools import reduce
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit
from typing import List
from typeguard import typechecked


@typechecked
def vertically_concatenate_datasets(dfs: List[DataFrame]):

    # We require datasets strictly to have the same columns
    # And the same types
    cols = set(dfs[0].columns)
    types = set(dfs[0].dtypes)
    for df in dfs:
        these_cols = set(df.columns)
        if these_cols != cols:
            raise ValueError(
                "Each dataframe in the list of dataframes provided to Splink"
                "must have strictly the same columns.  You provided one with "
                f"{these_cols} and another with {cols}"
            )
        these_types = set(df.dtypes)
        if these_types != types:
            raise ValueError(
                "Each dataframe in the list of dataframes provided to Splink"
                "must have strictly the same columns with same data types. "
                " You provided one with "
                f"{these_types} and another with {types}"
            )

    # UnionAll concats by position not name
    col_order = dfs[0].columns
    dfs = [df.select(col_order) for df in dfs]

    df = reduce(DataFrame.unionAll, dfs)

    return df
