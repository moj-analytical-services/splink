# The Fellegi-Sunter model

Splink implements the Fellegi-Sunter model for probabilistic record linkage. This topic guide is intended to give a high-level introduction to the model. If you aren't familiar with the concept probabilistic record linkage, it is recommended to read that [topic guide](./probabilistic_vs_deterministic.md) first.

<hr>

## Parameters of the Fellegi-Sunter model

The Fellegi-Sunter model has three main parameters that need to be considered to generate a match probability between two records:

!!! note ""
    * **λ** - probability that any two records match 
    * **m** - probability of a given observation *given* the records are a match
    * **u** - probability of a given observation *given* the records are **not** a match

<hr>

### **λ** probability

The lambda (**λ**) parameter is the prior probability that any two records match. I.e. assuming no other knowledge of the data, how likely is a match? Or, as a formula:

$$
λ = Pr(RecordsMatch)
$$

This is the same for all records comparisons, but is highly dependent on:

* The total number of records  
* The number of duplicate records (more duplicates increases **λ**)  
* The overlap between datasets  
    * Two datasets covering the same cohort (high overlap, high **λ**)
    * Two entirely independent datasets (low overlap, low **λ**)

<hr>

### **m** probability

The **m** probability is the probability of a given observation *given* the records are a match. Or, as a formula:

$$
m = Pr(Observation | RecordsMatch)
$$

For example, consider the the **m** probability of Date of Birth (DOB). For two records that are a match, what is the probability that:

**DOB is the same**:

Almost 100%, say 98% :arrow_right:  $m = 0.98$

**DOB is different**:

Maybe a 2% chance of a data error? :arrow_right: $m = 0.02$


The **m** probability is largely a measure of data quality - if DOB is poorly collected, it may only match exactly for 50% of true matches.

<hr>

### **u** probability

The **u** probability is the probability of a given observation *given* the records are **not** a match. Or, as a formula:

$$
m = Pr(Observation | RecordsDoNotMatch)
$$

For example, consider the the **u** probability of Surname. For two records that are a match, what is the probability that:

**Surname is the same**:

Depending on the surname, <1%? :arrow_right:  $m = 0.005$

**Surname is different**:

Almost 100% :arrow_right: $m = 0.995$


The **u** probability is a measure of coincidence. As there are so many possible surnames, the chance of sharing the same surname with a randomly-selected person is small.

<hr>

## Interpreting **m** and **u**

In the case of a perfect unique identifier:

* A person is only assigned one such value - **m = 1** (match) or **0** (non-match)  
* A value is only ever assigned to one person - **u = 0** (match) or **1** (non-match)

Where m and u deviate from these ideals can usually be intuitively explained:

!!! question ""
    ### **m** probability
    A measure of **data quality/reliability**.

    How often might a preson's information change legitimately or through data error? 

    * Names: typos, aliases, nicknames, middle names, married names etc.
    * DOB: typos, estimates (e.g. 1st Jan YYYY where date not known)
    * Address: formatting issues, moving house, multiple addresses, temporary addresses

!!! bug ""
    ### **u** probability
    A measure of **coincidence/cardinality**[^1].

    How many different people might share a given identifier?

    * DOB (high cardinality) – for a flat age distribution spanning ~30 years, there are ~10,000 DOBs (0.01% chance of a match)
    * Sex (low cardinality) – only 2 potential values (~50% chance of a match)

[^1]: 
    Cardinality is the the number of items in a set. In record linkage, cardinality refers to the number of possible values a feature could have.
    This is important in records linkage, as the number of possible options for e.g. date of birth has a significant impact on the amount of evidence that a match on date of birth provides for two records being a match.

<hr>

## Match Weights

One of the key measures of evidence of a match between records is the match weight. 

### Deriving Match Weights from **m** and **u**

The match weight is a measure of the relative size of **m** and **u**:

\begin{align}
    M &= log_2\left(\frac{λ}{1-λ}\right) + log_2(K) \\[10pt]
    &= log_2\left(\frac{λ}{1-λ}\right) + log_2\left(\frac{m}{u}\right)
\end{align}

where  
$λ$ is the probability that two random records match  
$K=m/u$ is the Bayes Factor

A key assumption of the Fellegi Sunter model is that observations from different column/comparisons are independent of one another. This means that the Bayes Factor for two records is the products of the Bayes Factor for each column/comparison.

$$K_{features} = K_{forename} \cdot K_{surname} \cdot K_{dob} \cdot K_{city} \cdot K_{email}$$

This, in turn, means that match weights are additive:

$$M_{obs} = M_{prior} + M_{features} $$

where 

$M_{prior} = log_2\left(\frac{λ}{1-λ}\right)$  
$M_{features} = M_{forename} + M_{surname} + M_{dob} + M_{city} + M_{email}$

So, considering these properties, the total Match Weight for two observed records can be rewritten as:

\begin{align}
    M_{obs} &= log_2\left(\frac{λ}{1-λ}\right) + \sum_{i}^{features}log_2(\frac{m_{i}}{u_{i}}) \\[10pt]
    &= log_2\left(\frac{λ}{1-λ}\right) + \log_2(\prod_{i}^{features}\frac{m_{i}}{u_{i}})
\end{align}



### Interpreting Match Weights

The Match Weight is the central metric showing the amount of evidence of a match is provided by each of the features in a model.  
The is most easily shown through Splink's Waterfall Chart:

![](../img/fellegi_sunter/waterfall.png)

1️⃣ are the two records being compared

2️⃣ is the match weight of the **prior**

$M_{prior} = log_2\left(\frac{λ}{1-λ}\right)$

This is the match weight if no additional knowledge of features is taken into account, and can be thought of as similar to the y-intercept in a simple regression.

3️⃣ are the match weights of **each feature** 

$M_{forename}$, $M_{surname}$, $M_{dob}$, $M_{city}$ and $M_{email}$ respectively.

4️⃣ is the **total** match weight for two observed records, combining 2️⃣ and 3️⃣

$M_{obs} = M_{prior} + M_{forename} + M_{surname} + M_{dob} + M_{city} + M_{email}$

$M_{obs} = -6.67 + 4.74 + 6.49 - 1.97 - 1.12 + 8.00 = 9.48$

5️⃣ is an axis representing the match weight (as Match Weight = log2(Bayes Factor))

6️⃣ is an axis representing the equivalent match probability (noting the non-linear scale). For more on the relationship between Match Weight and Probability, see the [sections below](#understanding-the-relationship-between-match-probability-and-match-weight)

<hr>

## Match Probability

Match probability is a more intuitive measure of similarity that match weight, and is, generally, used when choosing a similarity threshold for record matching.

### Deriving Match Probability from Match Weight

Probability of two records being a match can be derived from the total match weight:

$$
P(match|obs) = \frac{2^{M_{obs}}}{1+2^{M_{obs}}}
$$

???+ example "Example"
    Consider the example in the [Interpreting Match Weights](#interpreting-match-weights) section. 
    The total match weight, $M_{obs} = 9.48$. Therefore,

    $$ P(match|obs) = \frac{2^{9.48}}{1+2^{9.48}} \approx 0.999 $$

#### Understanding the relationship between Match Probability and Match Weight

It can be helpful to build up some inuition for how Match Weights translate into Match Probabilities. 

Plotting Match Probability versus Match Weight gives the following chart:

![](../img/fellegi_sunter/prob_v_weight.png)

Some observations from this chart:

* Match Weight = 0 &nbsp;&nbsp;&nbsp; :arrow_right: &nbsp;&nbsp;&nbsp; Match Probability = 0.5 
* Match Weight = 2 &nbsp;&nbsp;&nbsp; :arrow_right: &nbsp;&nbsp;&nbsp; Match Probability $\approx$ 0.8
* Match Weight = 3 &nbsp;&nbsp;&nbsp; :arrow_right: &nbsp;&nbsp;&nbsp; Match Probability $\approx$ 0.9
* Match Weight = 4 &nbsp;&nbsp;&nbsp; :arrow_right: &nbsp;&nbsp;&nbsp; Match Probability $\approx$ 0.95
* Match Weight = 7 &nbsp;&nbsp;&nbsp; :arrow_right: &nbsp;&nbsp;&nbsp; Match Probability $\approx$ 0.99

So, the impact of any additional Match Weight on Match Probability gets smaller as the total Match Weight increases. This makes intuitive sense as, when comparing two records, after you already have a lot of evidence/features indicating a match, adding more evidence/features will not have much of an impact on the probability of a match.

Similarly, if you already have a lot of negative evidence/features indicating a match, adding more evidence/features will not have much of an impact on the probability of a match.

### Deriving Match Probability from **m** and **u**

Given the definitions for Probability and Match Weights above, we can rewrite Probability in terms of **m** and **u**.

\begin{align}
P(match|obs) &= \frac{2^{log_2\left(\frac{λ}{1-λ}\right) + \log_2(\prod_{i}^{features}\frac{m_{i}}{u_{i}})}}{1+2^{log_2\left(\frac{λ}{1-λ}\right) + \log_2(\prod_{i}^{features}\frac{m_{i}}{u_{i}})}} \\[10pt]
&= \frac{\left(\frac{λ}{1-λ}\right)\prod_{i}^{features}\frac{m_{i}}{u_{i}}}{1+\left(\frac{λ}{1-λ}\right)\prod_{i}^{features}\frac{m_{i}}{u_{i}}} \\[10pt]
&= 1 - \frac{1}{1+\left(\frac{λ}{1-λ}\right)\prod_{i}^{features}\frac{m_{i}}{u_{i}}}
\end{align}



<hr>



## Further Reading

!!! info ""
    For a more in-depth introduction to the Fellegi-Sunter model, see Robin Linacre's [**Interactive Blogs**](https://www.robinlinacre.com/probabilistic_linkage/) including:

    * [The mathematics of the Fellegi Sunter model](https://www.robinlinacre.com/maths_of_fellegi_sunter/)
    * [Visualising the Fellegi Sunter Model](https://www.robinlinacre.com/visualising_fellegi_sunter/)
    * [Understanding match weights](https://www.robinlinacre.com/understanding_match_weights/)
    * [Dependencies between match weights](https://www.robinlinacre.com/match_weight_dependencies/)
    * [m and u probability generator](https://www.robinlinacre.com/m_u_generator/)

    Also, see the [academic paper](https://imai.fas.harvard.edu/research/files/linkage.pdf) used as the basis for a similar implementation of Fellegi Sunter in the R [fastlink package](https://github.com/kosukeimai/fastLink).

