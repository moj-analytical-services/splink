---
date: 2022-07-27
authors:
  - ross-k
categories:
  - Feature Updates
---

# Splink Updates - December 2023

Welcome to the second installment of the Splink Blog! 

Here are some of the highlights from the second half of 2023, and a taste of what is in store for 2024!

<!-- more -->

Latest Splink version: [v3.9.9](https://github.com/moj-analytical-services/splink/releases/tag/v3.9.9)

## :bar_chart: Charts Gallery

The Splink docs site now has a [Charts Gallery](../../charts/index.md) to show off all of the charts that come out-of-the-box with Splink to make linking easier. Each chart now has an explanation of:

1. What the chart shows
2. How to interpret it
3. Actions to take as a result

This is the first step on a longer term journey to provide more guidance on how to evaluate Splink models and linkages, so watch this space for more in the coming months!

## :chart_with_upwards_trend: New Charts

We are always adding more charts to Splink - to understand how these charts are built see our new [Charts Developer Guide](../../dev_guides/charts/understanding_and_editing_charts.md).

Two of our latest additions are:

### :material-matrix: Confusion Matrix

When evaluating any classification model, a confusion matrix is a useful tool for summarizing performance by representing counts of true positive, true negative, false positive, and false negative predictions.

Splink now has its own [confusion matrix chart](../../charts/confusion_matrix_from_labels_table.ipynb) to show how performance changes with a given match weight threshold. Note, labelled data is required to generate 

### :material-table: Completeness Chart

When linking multiple datasets together, one of the most important factors for a successful linkage is the number 

## :electric_plug: Backend Specific Installs

Adding pot

For example, to install for Spark use the command:

```bsh
pip install 'splink[spark]'
```

## :clipboard: Settings Validation

The [Settings dictionary](../../settings_dict_guide.md) is central to everything in Splink. It defines everything from the sql dialect of your backend to how features are compared in Splink model. However, users have had problems 

## :no_entry_sign: Drop support for python 3.7

From Splink 3.9.7, support has been dropped for python 3.7. This decision has been made to simplify managing dependencies in the back end of Splink.

If you are working with python 3.7, please revert to Splink 3.9.6.

```bsh
pip install splink==3.9.6
```

## :fast_forward: Speed up tests

## :simple-adblock: Blocking Rule Library (Improved)




## :soon: What's in the pipeline?

* :four:   Work on **Splink 4** is currently underway
* :material-thumbs-up-down:   More guidance on how to evaluate Splink models and linkages




