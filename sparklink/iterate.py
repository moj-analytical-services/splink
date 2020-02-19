# For type hints. Try except to ensure the sql_gen functions even if spark doesn't exist.
try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None

from .expectation_step import run_expectation_step
from .maximisation_step import run_maximisation_step
from .params import Params
from .check_types import check_types

import logging

log = logging.getLogger(__name__)
from .logging_utils import log_sql, log_other

@check_types
def iterate(
    df_gammas: DataFrame,
    params: Params,
    settings: dict,
    spark: SparkSession,
    log_iteration:bool=False,
    num_iterations:int=10,
    compute_ll:bool=False,
):
    """Repeatedly run expectation and maximisation step until convergence or max itations is reached.

    Args:
        df_gammas (DataFrame): Spark dataframe including gamma columns
        params (Params): The `sparklink` params object
        settings (dict): The `sparklink` settings dictionary
        spark (SparkSession): The SparkSession object
        log_iteration (bool, optional): Whether to write a message to the log after each iteration. Defaults to False.
        num_iterations (int, optional): The number of iterations to run. Defaults to 10.
        compute_ll (bool, optional): Whether to compute the log likelihood.  This is not necessary and significantly degrades performance. Defaults to False.

    Returns:
        DataFrame: A spark dataframe including a match probability column
    """

    df_gammas.persist()
    for i in range(num_iterations):
        df_e = run_expectation_step(
            df_gammas, params, settings, spark, compute_ll=compute_ll
        )

        # To align to the parameters in the iteration chart, LL must be computed against
        # pi and lambda pre-maximisation
        # compute_log_likelihood()

        run_maximisation_step(df_e, params, spark)
        if log_iteration:
            log_other(f"Iteration {i} complete", logger=log, level="INFO")

    # The final version of df_e should align to the current parameters - i.e. those computed in the last max step
    df_e = run_expectation_step(
        df_gammas, params, settings, spark, compute_ll=compute_ll
    )

    df_gammas.unpersist()
    return df_e

