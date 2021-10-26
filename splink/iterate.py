from typing import Callable
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from .expectation_step import run_expectation_step
from .maximisation_step import run_maximisation_step
from .model import Model
from typeguard import typechecked

import logging

logger = logging.getLogger(__name__)


@typechecked
def iterate(
    df_gammas: DataFrame,
    model: Model,
    spark: SparkSession,

    save_state_fn: Callable = None,
):
    """Repeatedly run expectation and maximisation step until convergence or max itations is reached.

    Args:
        df_gammas (DataFrame): Spark dataframe including gamma columns (i.e. after blocking and add_gammas has been applied)
        model (Model): The `splink` model object
        spark (SparkSession): The SparkSession object
        log_iteration (bool, optional): Whether to write a message to the log after each iteration. Defaults to False.
        num_iterations (int, optional): The number of iterations to run. Defaults to 10.

        save_state_fn (function, optional):  A function provided by the user that takes one arguments, params, and is executed each iteration.  This is a hook that allows the user to save the state between iterations, which is mostly useful for very large jobs which may need to be restarted from where they left off if they fail.

    Returns:
        DataFrame: A spark dataframe including a match probability column
    """
    settings = model.current_settings_obj.settings_dict
    num_iterations = settings["max_iterations"]
    for i in range(num_iterations):

        df_e = run_expectation_step(df_gammas, model, spark)

        run_maximisation_step(df_e, model, spark)

        logger.info(f"Iteration {i} complete")

        if save_state_fn:
            save_state_fn(model)
        if model.is_converged():
            logger.info("EM algorithm has converged")
            break

    # The final version of df_e should align to the current parameters - i.e. those computed in the last max step
    df_e = run_expectation_step(df_gammas, model, spark)

    # The expectation step adds the current params to history, so this is needed to output a final
    # version of charts/params.
    if save_state_fn:
        save_state_fn(model)

    return df_e
