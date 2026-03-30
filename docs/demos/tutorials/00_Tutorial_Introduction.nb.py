# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Introductory tutorial
#
# This is the introduction to a seven part tutorial which demonstrates how to de-duplicate a small dataset using simple settings.
#
# The aim of the tutorial is to demonstrate core Splink functionality succinctly, rather that comprehensively document all configuration options.
#
# The seven parts are:
#
# - [1. Data prep pre-requisites](./01_Prerequisites.ipynb)
#
# - [2. Exploratory analysis](./02_Exploratory_analysis.ipynb) <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/02_Exploratory_analysis.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# - [3. Choosing blocking rules to optimise runtimes](./03_Blocking.ipynb) <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/03_Blocking.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# - [4. Estimating model parameters](./04_Estimating_model_parameters.ipynb) <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/04_Estimating_model_parameters.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# - [5. Predicting results](./05_Predicting_results.ipynb) <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/05_Predicting_results.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# - [6. Visualising predictions](./06_Visualising_predictions.ipynb) <a> <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/06_Visualising_predictions.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# - [7. Evaluation](./07_Evaluation.ipynb) <a> <a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/tutorials/07_Evaluation.ipynb">
#   <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
# </a>
#
# - [8. Building your own model](./08_building_your_own_model.md) 
#
#
# Throughout the tutorial, we use the duckdb backend, which is the recommended option for smaller datasets of up to around 1 million records on a normal laptop.
#
# You can find these tutorial notebooks in the `docs/demos/tutorials/` folder of the  [splink repo](https://github.com/moj-analytical-services/splink/tree/master/docs/demos/tutorials), or click the Colab links to run in your browser.

# %% [markdown]
# ## End-to-end demos
#
# After following the steps of the tutorial, it might prove useful to have a look at some of the [example notebooks](https://moj-analytical-services.github.io/splink/demos/examples/examples_index.html) that show various use-case scenarios of Splink from start to finish.
#
# ## Interactive Introduction to Record Linkage Theory
#
# If you'd like to learn more about record linkage theory, an interactive introduction is available [here](https://www.robinlinacre.com/intro_to_probabilistic_linkage/).

# %% [markdown] vscode={"languageId": "plaintext"}
# ## LLM prompts
#
# If you're using an LLM to suggest Splink code, see [here](../../topic_guides/llms/prompting_llms.md) for suggested prompts and context.
