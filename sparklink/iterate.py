from .expectation_step import run_expectation_step
from .maximisation_step import run_maximisation_step

import logging

log = logging.getLogger(__name__)
from .logging_utils import log_sql, log_other

def iterate(df_gammas, spark, params, log_iteration=False, num_iterations=10, compute_ll=False):
    df_gammas.persist()
    for i in range(num_iterations):
        df_e = run_expectation_step(df_gammas, spark, params, compute_ll=compute_ll)

        # To align to the parameters in the iteration chart, LL must be computed against
        # pi and lambda pre-maximisation
        # compute_log_likelihood()

        run_maximisation_step(df_e, spark, params)
        if log_iteration:
            log_other(f"Iteration {i} complete", logger=log, level='INFO')

    # The final version of df_e should align to the current parameters - i.e. those computed in the last max step
    df_e = run_expectation_step(df_gammas, spark, params, compute_ll=compute_ll)

    df_gammas.unpersist()
    return df_e

