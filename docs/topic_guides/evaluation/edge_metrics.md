This guide is intended to be a reference guide for Edge Metrics used throughout Splink. It will build up from basic principles into more complex metrics.

!!! note
    All of these metrics are dependent on having a "ground truth" to compare against. This is generally provided by **clerical labelling** (i.e. labels created by a human). For more on how to generate this ground truth (and the impact that can have on Edge Metrics), check out the [Clerical Labelling Topic Guide](./labelling.md).

## The Basics

Any Edge (Link) within a Splink model will fall into one of four categories:

#### True Positive

A True Positive is a case where a Splink model correctly identifies a match between two records.

#### True Negative

A True Negative is a case where a Splink model correctly identifies a non-match between two records.

#### False Positive

A False Positive is a case where a Splink model incorrectly predicts a match between two records, when they are actually a non-match.

Also referred to as a Type I Error.

#### False Negative

A False Negative is a case where a Splink model incorrectly predicts a non-match between two records, when they are actually a match.

Also referred to as a Type II Error

### Confusion Matrix
These can be summarised in a Confusion Matrix

![](./image/confusion_matrix.drawio.png){:style="width:600px"}
In a perfect model there would be no False Positives or False Negatives (i.e. FP = 0 and FN = 0).

## Metrics for Linkage

The confusion matrix shows **counts** of each link type, but we are generally more interested in **proportions**. I.e. what percentage of the time does the model get the answer right?

### Accuracy

The simplest metric is 

$$\textsf{Accuracy} = \frac{\textsf{True Positives}+\textsf{True Negatives}}{\textsf{All Predictions}}$$

This measures the proportion of correct classifications (of any kind). This may be useful for balanced data but high accuracy can be achieved by simply assuming the majority class for highly imbalanced data (e.g. assuming non-matches).

<hr>

![](./image/confusion_matrix_extra.drawio.png)


### True Positive Rate (Recall)

The True Positve Rate, or Recall, is the proportion of matches that are correctly predicted by Splink.

$$\textsf{Recall} = \frac{\textsf{True Positives}}{\textsf{All Positives}} = \frac{\textsf{True Positives}}{\textsf{True Positives} + \textsf{False Negatives}}$$

### True Negative Rate (Specificity)

The True Negative Rate, or Specificity, is the proportion of non-matches that are correctly predicted by Splink.

$$\textsf{Specificity} = \frac{\textsf{True Negatives}}{\textsf{All Negatives}} = \frac{\textsf{True Negatives}}{\textsf{True Negatives} + \textsf{False Positives}}$$

### Positive Predictive Value (Precision)

The Positive Predictive Value, or Precision, is the proportion of predicted matches which are true matches.

$$\textsf{Precision} = \frac{\textsf{True Positives}}{\textsf{All Predicted Positives}} = \frac{\textsf{True Positives}}{\textsf{True Positives} + \textsf{False Negatives}}$$

### Negative Predictive Value

The Negative Predictive Value is the proportion of predicted non-matches which are true non-matches.

$$\textsf{Negative Predictive Value} = \frac{\textsf{True Negatives}}{\textsf{All Predicted Negatives}} = \frac{\textsf{True Negatives}}{\textsf{True Negatives} + \textsf{False Positives}}$$


!!! warning 

    Each of these metrics looks at just one row or column of the confusion matrix. A model cannot be meaningfully summarised by just one of these performance measures.

    **“Predicts cancer with 100% Precision”** - is true of a “model” that correctly identifies one known cancer patient, but misdiagnoses everyone else as cancer-free.

    **“AI judge’s verdicts have Recall of 100%”** - is true for a power-mad AI judge that declares everyone guilty, which would be stoically described by civil servants as “sub-optimal”.

## Composite Metrics for Linkage

This section contains composite metrics i.e. combinations of metrics that can been derived from the confusion matrix (Precision, Recall, Specificity and Negative Predictive Value). 

Any comparison of two records has a number of possible outcomes (True Positives, False Positives etc.), each of which has a different impact on your specific use case. It is very rare that a single metric defines the desired behaviour of a model. Therefore, evaluating performance with a composite metric (or a combination of metrics) is advised.

### F Score

The F-Score is a weighted harmonic mean of Precision (PPV) and Recall (TPR). For a a general weight $\beta$:

$$F_{\beta} = \frac{(1 + \beta^2) \cdot \textsf{Precision} \cdot \textsf{Recall}}{\beta^2 \cdot \textsf{Precision} + \textsf{Recall}}$$

where Recall is $\beta$ times more important than Precision.

For example, when Precision and Recall are equally weighted ($\beta = 1$), we get:

$$F_{1} = 2\left[\frac{1}{\textsf{Precision}}+\frac{1}{\textsf{Recall}}\right]^{-1} = \frac{2 \cdot \textsf{Precision} \cdot \textsf{Recall}}{\textsf{Precision} + \textsf{Recall}}$$

Other popular versions of the F score are $F_{2}$ (Recall twice as important as Precision) and $F_{0.5}$ (Precision twice as important as Recall)

!!! warning

    F-score does not account for class imbalance in the data, and is asymmetric (i.e. it considers the prediction of matching records, but ignores how well the model correctly predicts non-matching records).

### P4 Score

$P_{4}$ is the harmonic mean of the 4 metrics that can be directly derived from the confusion matric:

$$ 4\left[\frac{1}{\textsf{Recall}}+\frac{1}{\textsf{Specificity}}+\frac{1}{\textsf{Precision}}+\frac{1}{\textsf{Negative Predictive Value}}\right]^{-1} $$

This addresses one of the issues with the F-Score as it considers how well the model predicts non-matching records as well as matching records.

Note here that all metrics are given equal weighting.

### Matthews Correlation Coefficient (MCC)

The Matthews Correlation Coefficient ($\phi$) is a measure of how correlation between predictions and actual observations.

$$ \phi = \sqrt{\textsf{Recall} \cdot \textsf{Specificity} \cdot \textsf{Precision} \cdot \textsf{Negative Predictive Value}} - \sqrt{(1 - \textsf{Recall})(1 - \textsf{Specificity})(1 - \textsf{Precision})(1 - \textsf{Negative Predictive Value})} $$

!!! note

    Unlike the other metrics in this guide, $\phi$ is a correlation coefficient, so can range from -1 to 1 (as opposed to a range of 0 to 1).

    In reality, linkage models should never be negatively correlated with actual observations, so $\phi$ can be used in the same way as other metrics.