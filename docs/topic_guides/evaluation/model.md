# Model Evaluation

The parameters in a trained Splink model determine the match probability (Splink score) assigned to pairwise record comparisons. Before scoring any pairs of records there are a number of ways to check whether your model will perform as you expect.

## Look at the model parameters

The final model is summarised in the [match weights chart](../../charts/match_weights_chart.ipynb) with each bar in the chart signifying the match weight (i.e. the amount of evidence for or against a match) for each comparison level in your model.

If, after some investigation, you still can't make sense of some of the match weights, take a look at the corresponding $m$ and $u$ values generated to see if they themselves make sense. These can be viewed in the [m u parameters chart](../../charts/m_u_parameters_chart.ipynb).

!!! info ""

    Remember that $\textsf{Match Weight} = \log_2 \frac{m}{u}$


## Look at the model training

The behaviour of a model during training can offer some insight into its utility. The more stable a model is in the training process, the more reliable the outputs are.

Stability of model training can be seen in the Expectation Maximisation stage (for $m$ training):

- Stability across EM training sessions can be seen through the [parameter estimates chart](../../charts/parameter_estimate_comparisons_chart.ipynb)

- Stability within each session is indicated by the speed of convergence of the algorithm. This is shown in the terminal output during training. In general, the fewer iterations required to converge the better.  You can also access convergence charts on the [EM training session object](../../api_docs/em_training_session.md)

    ```python
    training_session = linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name", "surname")
    )
    training_session.match_weights_interactive_history_chart()
    ```



## In summary

Evaluating a trained model is not an exact science - there are no metrics which can definitively say whether a model is good or bad at this stage. In most cases, applying human logic and heuristics is the best you can do to establish whether the model is sensible. Given the variety of potential use cases of Splink, there is no perfect, universal model, just models that can be tuned to produce useful outputs for a given application.

The tools within Splink are intended to help identify areas where your model may not be performing as expected. In future versions releases we hope to automatically flag where there are areas of a model that require further investigation to make this process easier for the user.