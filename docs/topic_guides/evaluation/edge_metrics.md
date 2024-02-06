This guide is intended to be a reference guide for Edge Metrics used throughout Splink. It will build up from basic principles into more complex metrics.

## The Basics

Any Edge (Link) within a Splink model will fall into one of four categories:

#### 1. True Positive

A True Positive is a case where a Splink model correctly identifies a match between two records.

#### 2. True Negative

A True Negative is a case where a Splink model correctly identifies a non-match between two records.

#### 3. False Positive

A False Positive is a case where a Splink model incorrectly predicts a match between two records, when they are actually a non-match.

Also referred to as a Type I Error.

#### 4. False Negative

A False Negative is a case where a Splink model incorrectly predicts a non-match between two records, when they are actually a match.

Also referred to as a Type II Error

### Confusion Matrix
These can be summarised in a Confusion Matrix

![](./image/confusion_matrix.drawio.png){:style="width:600px"}
In a perfect model there would be no False Positives or False Negatives (i.e. FP = 0 and FN = 0).

## Metrics for Linkage

The confusion matrix shows **counts** of each link type, but we are generally more interested in **proportions**. I.e. what percentage of the time does the model get the answer right?


![](./image/confusion_matrix_extra.drawio.png)

### True Positive Rate (Recall)

The True Positve Rate, or Recall, is the proportion of matches that are correctly predicted by Splink. I.e. $\textsf{Recall} = \frac{\textsf{True Positives}}{\textsf{All Positives}} = \frac{\textsf{True Positives}}{\textsf{True Positives} + \textsf{False Negatives}}$
