# The Fellegi-Sunter model

Splink implements the Fellegi-Sunter model for probabilistic record linkage. This topic guide is intended to give a high-level introduction to the model. If you aren't familiar with the concept probabilistic record linkage, it is recommended to read that [topic guide](./probabilistic_vs_deterministic.md) first.

<hr>

## Parameters of the Fellegi-Sunter model

The Fellegi-Sunter model has three main parameters that need to be considered to generate a match probability between two records:

* **λ** - probability that any two records match 
* **m** - probability of a given observation *given* the records are a match
* **u** - probability of a given observation *given* the records are **not** a match

<hr>

### **λ** probability

The lambda (**λ**) parameter is the prior probability that any two records match. I.e. assuming no other knowledge of the data, how likely is a match?

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

<div style="display: flex;">
  <div style="flex: 1;">
    <strong>m</strong>-probability <br>
    A measure of <u>data quality/reliability</u>. <br>
    How often might a preson's information change legitimately or through data error? 
  </div>
  <div style="flex: 1;">
    <strong>u</strong>-probability <br>
    A measure of <u>coincidence/cardinality</u>. <br>
    How many different people might share a given identifier? 
  </div>
</div>
<div style="display: flex;">
  <div style="flex: 1;">
    Examples: <br>
    - <strong>Names:</strong> typos, aliases, nicknames, middle names, married names etc. <br>
    - <strong>DOB:</strong> typos, estimates (e.g. 1st Jan YYYY where date not known) <br>
    - <strong>Address:</strong> formatting issues, moving house, multiple addresses, temporary addresses

  </div>
  <div style="flex: 1;">
    Examples: <br>
    - <strong>DOB:</strong> or a flat age distribution spanning ~30 years, there are ~10,000 DOBs (0.01% chance of a match)<br>
    - <strong>Sex:</strong> only 2 potential values (~50% chance of a match)
  </div>
</div>


<hr>

## Further Reading

!!! info ""
    For a more in-depth introduction to the Fellegi-Sunter model, see Robin Linacre's [**Interactive Blogs**](https://www.robinlinacre.com/probabilistic_linkage/) including:

    * [The mathematics of the Fellegi Sunter model](https://www.robinlinacre.com/maths_of_fellegi_sunter/)
    * [Visualising the Fellegi Sunter Model](https://www.robinlinacre.com/visualising_fellegi_sunter/)
    * [Understanding match weights](https://www.robinlinacre.com/understanding_match_weights/)
    * [Dependencies between match weights](https://www.robinlinacre.com/match_weight_dependencies/)
    * [m and u probability generator](https://www.robinlinacre.com/m_u_generator/)