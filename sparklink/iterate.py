from .expectation_step import run_expectation_step
from .maximisation_step import run_maximisation_step

def iterate(df_gammas, spark, params, print_iteration=False, num_iterations=10, print_ll=False):
    df_gammas.persist()
    for i in range(num_iterations):
        df_e = run_expectation_step(df_gammas, spark, params, print_ll=print_ll)
        run_maximisation_step(df_e, spark, params)
        if print_iteration:
            print(params)

    df_gammas.unpersist()
    return df_e
