from expectation_step import run_expectation_step
from maximisation_step import run_maximisation_step

def iterate(df_gammas, spark, params, print_iteration=False, num_iterations=10):
    for i in range(num_iterations):
        df_e = run_expectation_step(df_gammas, spark, params)
        run_maximisation_step(df_e, spark, params)
        if print_iteration:
            print(params)
    return df_e
