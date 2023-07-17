# Training Linkage models in Splink

Within Splink there are a number of ways to train the parameters of the Fellegi-Sunter model. The topic guides in this section will cover the options for training the following parameters:

* $\lambda$ - probability that any two records match 
* $m$ - probability of a given observation *given* the records are a match
* $u$ - probability of a given observation *given* the records are **not** a match

including the pros and cons for each methodology.

For a theoretical understanding of each of these parameters and how they combine to generate results in a Splink model, please refer to the [Fellegi-Sunter Topic Guide](../theory/fellegi_sunter.md)

## What considerations should be made when training a model?

* accuracy of a model
* stability of a model
* reproducibility of a model
* processing time
* information available

