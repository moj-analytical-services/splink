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

logger = logging.getLogger(__name__)
from typing import Callable

@check_types
def iterate(
    df_gammas: DataFrame,
    params: Params,
    settings: dict,
    spark: SparkSession,
    compute_ll:bool=False,
    save_state_fn:Callable=None
):
    """Repeatedly run expectation and maximisation step until convergence or max itations is reached.

    Args:
        df_gammas (DataFrame): Spark dataframe including gamma columns
        params (Params): The `splink` params object
        settings (dict): The `splink` settings dictionary
        spark (SparkSession): The SparkSession object
        log_iteration (bool, optional): Whether to write a message to the log after each iteration. Defaults to False.
        num_iterations (int, optional): The number of iterations to run. Defaults to 10.
        compute_ll (bool, optional): Whether to compute the log likelihood.  This is not necessary and significantly degrades performance. Defaults to False.
        save_state_fn (function, optional):  A function provided by the user that takes two arguments, params and settings, and is executed each iteration.  This is a hook that allows the user to save the state between iterations, which is mostly useful for very large jobs which may need to be restarted from where they left off if they fail.

    Returns:
        DataFrame: A spark dataframe including a match probability column
    """

    num_iterations = settings["max_iterations"]
    for i in range(num_iterations):
        df_e = run_expectation_step(
            df_gammas, params, settings, spark, compute_ll=compute_ll
        )

        run_maximisation_step(df_e, params, spark)

        logger.info(f"Iteration {i} complete")

        if save_state_fn:
            save_state_fn(params, settings)
        if params.is_converged():
            logger.info("EM algorithm has converged")
            break

    # The final version of df_e should align to the current parameters - i.e. those computed in the last max step
    df_e = run_expectation_step(
        df_gammas, params, settings, spark, compute_ll=compute_ll
    )

    return df_e
