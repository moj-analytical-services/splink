---
tags:
  - API
  - comparisons
  - Levenstein
  - Damerau-Levenshtein
  - Jaro
  - Jaro-Winkler
  - Jaccard
---
# String Comparators

A Splink model contains a collection of `Comparisons` and `ComparisonLevels` organised in a hierarchy.  For example:

```
Data Linking Model
├─-- Comparison: Date of birth
│    ├─-- ComparisonLevel: Exact match
│    ├─-- ComparisonLevel: Up to one character difference
│    ├─-- ComparisonLevel: Up to three character difference
│    ├─-- ComparisonLevel: All other
├─-- Comparison: Name
│    ├─-- ComparisonLevel: Exact match on first name and surname
│    ├─-- ComparisonLevel: Exact match on first name
│    ├─-- etc.
```

For more detail on how comparisons are constructed, see the dedicated [topic guide](customising_comparisons.ipynbcustomising_comparisons.ipynb) as well as fuller descriptions of [`Comparisons`](../comparison.md) and [`Comparison Levels`](../comparison_level.md). 

Within `Comparisons` it is useful for different `Comparison Levels` to allow for different styles (and levels) fuzzy match. Each of these `Comparison Levels` indicates a different class of match between two records and therefore a different type (and amount) of evidence for or against the two records being a match. Once these `Comparison Levels` have been defined, the Splink model is trained to estimate the Match Weight to assign to each `Comparison Level`.

There are a number of string comparator functions available in Splink that allow fuzzy matching for strings within `Comparisons` and `Comparison Levels`. For each of these fuzzy matching functions, below you will find explanations of how they work, worked examples and recommendations for the types of data they are useful for.

For guidance on how to choose the most suitable string comparator, and associated threshold, see the dedicated [topic guide](./choosing_comparators.ipynb).

<hr>

## Levenshtein Distance

!!! info "At a glance" 
    **Useful for:** Data entry errors e.g. character miskeys.  
    **Splink comparison functions:** [levenshtein_level()](../comparison_level_library.md#splink.comparison_level_library.LevenshteinLevelBase) and [levenshtein_at_thresholds()](../comparison_library.md#splink.comparison_library.LevenshteinAtThresholdsBase)  
    **Returns:** An integer (lower is more similar).

##### Description
Levenshtein distance, also known as edit distance, is a measure of the difference between two strings. It represents the minimum number of **insertions**, **deletions**, or **substitutions** of characters required to transform one string into the other.

Or, as a formula,

$$Levenstein(s_1, s_2) = \min \lbrace \begin{array}{l}
\text{insertion} \ ,
\text{deletion} ,
\text{substitution} 
\end{array} \rbrace $$

##### Examples

??? example "**"KITTEN" vs "SITTING"**"

    The minimum number of operations to convert "KITTEN" into "SITTING" are:

    - Substitute "K" in "KITTEN" with "S" to get "SITTEN."
    - Substitute "E" in "SITTEN" with "I" to get "SITTIN."
    - Insert "G" after "N" in "SITTIN" to get "SITTING."

    Therefore, 

    $$Levenstein(KITTEN, SITTING) = 3$$

??? example "**"CAKE" vs "ACKE"**"

    The minimum number of operations to convert "CAKE" into "ACKE" are:

    - Substitute "C" in "CAKE" with "A" to get "AAKE."
    - substitute the second "A" in "AAKE" with "C" to get "ACKE."

    Therefore, 

    $$Levenstein(CAKE, ACKE) = 2$$

##### Sample code

You can test out the Levenshtein distance between two strings through the [jellyfish](https://jamesturk.github.io/jellyfish/) package.

```python
import jellyfish
levenshtein_distance("CAKE", "ACKE)
```
> 2

<hr>

## Damerau-Levenshtein Distance

!!! info "At a glance" 
    **Useful for:** Data entry errors e.g. character transpositions and miskeys  
    **Splink comparison functions:** [damerau_levenshtein_level()](../comparison_level_library.md#splink.comparison_level_library.DamerauLevenshteinLevelBase) and [damerau_levenshtein_at_thresholds()](../comparison_library.md#splink.comparison_library.DamerauLevenshteinAtThresholdsBase)  
    **Returns:** An integer (lower is more similar).

##### Description
Damerau-Levenshtein distance is a variation of [Levenshtein distance](#levenshtein-distance) that also includes transposition operations, which are the interchange of adjacent characters. This distance measures the minimum number of operations required to transform one string into another by allowing **insertions**, **deletions**, **substitutions**, and **transpositions** of characters.

Or, as a formula,

$$DamerauLevenstein(s_1, s_2) = \min \lbrace \begin{array}{l}
\text{insertion} \ ,
\text{deletion} ,
\text{substitution} ,
\text{transposition}
\end{array} \rbrace $$

##### Examples

??? example "**"KITTEN" vs "SITTING"**"

    The minimum number of operations to convert "KITTEN" into "SITTING" are:

    - Substitute "K" in "KITTEN" with "S" to get "SITTEN".
    - Substitute "E" in "SITTEN" with "I" to get "SITTIN".
    - Insert "G" after "T" in "SITTIN" to get "SITTING".

    Therefore, 

    $$DamerauLevenstein(KITTEN, SITTING) = 3$$

??? example "**"CAKE" vs "ACKE"**"

    The minimum number of operations to convert "CAKE" into "ACKE" are:

    - Transpose "C" and "A" in "CAKE" with "A" to get "ACKE."

    Therefore, 

    $$DamerauLevenstein(CAKE, ACKE) = 1$$

##### Sample code

You can test out the Damerau-Levenshtein distance between two strings through the [jellyfish](https://jamesturk.github.io/jellyfish/) package.

```python
import jellyfish
damerau_levenshtein_distance("CAKE", "ACKE)
```
> 1

<hr>


## Jaro Similarity

!!! info "At a glance" 
    **Useful for:**  Strings where all characters are considered equally important, regardless of order e.g. ID numbers  
    **Splink comparison functions:**  [jaro_level()](../comparison_level_library.md#splink.comparison_level_library.JaroLevelBase) and [jaro_at_thresholds()](../comparison_library.md#splink.comparison_library.JaroAtThresholdsBase)  
    **Returns:**  A score between 0 and 1 (higher is more similar).

##### Description
Jaro similarity is a measure of similarity between two strings. It takes into account the number and order of matching characters, as well as the number of transpositions needed to make the strings identical.

Jaro similarity considers:

- The number of matching characters (characters in the same position in both strings).
- The number of transpositions (pairs of characters that are not in the same position in both strings).

Or, as a formula,

$$Jaro = \frac{1}{3} \left[ \frac{m}{|s_1|} + \frac{m}{|s_2|} + \frac{m-t}{m} \right]$$

where $s_1$ and $s_2$ are the two strings being compared, 

$m$ is the number of common characters (which are considered matching only if they are the same and not farther than $\left\lfloor \frac{\min(|s_1|,|s_2|)}{2} \right\rfloor - 1$ characters apart), 

and $t$ is the number of transpositions (which is calculated as the number of matching characters that are not in the right order divided by two).

##### Examples

??? example "**"MARTHA" vs "MARHTA":**"

    - There are four matching characters: "M", "A", "R", and "T".
    - There is one transposition: the fifth character in "MARTHA" ("H") is not in the same position as the fifth character in "MARHTA" ("T").
    - We calculate the Jaro similarity using the formula:

    $$Jaro(MARTHA, MARHTA) = \frac{1}{3} \left[ \frac{4}{6} + \frac{(4)}{6} + \frac{4-1}{4} \right] = 0.944$$

??? example "**"MARTHA" vs "AMRTHA":**"

    - There are four matching characters: "M", "A", "R", and "T".
    - There is one transposition: the first character in "MARTHA" ("M") is not in the same position as the first character in "AMRTHA" ("T").
    - We calculate the Jaro similarity using the formula:

    $$Jaro(MARTHA, AMRTHA) = \frac{1}{3} \left[ \frac{4}{6} + \frac{(4)}{6} + \frac{4-1}{4} \right] = 0.944$$

    Noting that transpositions of strings gives the same Jaro similarity regardless of order.

##### Sample code

You can test out the Jaro similarity between two strings through the [jellyfish](https://jamesturk.github.io/jellyfish/) package.

```python
import jellyfish
jellyfish.jaro_similarity("MARTHA", "AMRTHA)
```
> 0.944

<hr>

## Jaro-Winkler Similarity

!!! info "At a glance" 
    **Useful for:** Strings where importance is weighted towards the first 4 characters e.g. Names  
    **Splink comparison functions:** [jaro_winkler_level()](../comparison_level_library.md#splink.comparison_level_library.JaroWinklerLevelBase) and [jaro_winkler_at_thresholds()](../comparison_library.md#splink.comparison_library.JaroWinklerAtThresholdsBase)  
    **Returns:**  A score between 0 and 1 (higher is more similar).


##### Description
Jaro-Winkler similarity is a variation of [Jaro similarity](#jaro-similarity) that gives extra weight to matching prefixes of the strings. It is particularly useful for names

The Jaro-Winkler similarity is calculated as follows:

$$Jaro Winkler = Jaro + p \cdot l \cdot (1 - Jaro)$$

where  
$Jaro$ is the [Jaro similarity](#jaro-similarity) between the two strings.  
$l$ is the length of the common prefix between the two strings, up to a maximum of four characters.  
$p$ is a prefix scale factor, commonly set to 0.1.

##### Examples

??? example  "**"MARTHA" vs "MARHTA"**"

    The common prefix between the two strings is "MAR", which has a length of 3.
    We calculate the Jaro-Winkler similarity using the formula:

    $$Jaro Winkler(MARTHA, MARHTA) = 0.944 + 0.1 \cdot 3 \cdot (1 - 0.944) = 0.9612$$

    The Jaro-Winkler similarity is slightly higher than the Jaro similarity, due to the matching prefix. 

??? example  "**"MARTHA" vs "AMRTHA":**"

    There is no common prefix, so the Jaro-Winkler similarity formula gives:

    $$Jaro Winkler(MARTHA, AMRTHA) = 0.944 + 0.1 \cdot 0 \cdot (1 - 0.944) = 0.944$$

    Which is the same as the Jaro score.

    Note that the Jaro-Winkler similarity should be used with caution, as it may not always provide better results than the standard Jaro similarity, especially when dealing with short strings or strings that have no common prefix.

##### Sample code

You can test out the Jaro similarity between two strings through the [jellyfish](https://jamesturk.github.io/jellyfish/) package.

```python
import jellyfish
jellyfish.jaro_winkler_similarity("MARTHA", "MARHTA)
```
> 0.9612

<hr>

## Jaccard Similarity

!!! info "At a glance" 
    **Useful for:**   
    **Splink comparison functions:** [jaccard_level()](../comparison_level_library.md#splink.comparison_level_library.JaccardLevelBase) and [jaccard_at_thresholds()](../comparison_library.md#splink.comparison_library.JaccardAtThresholdsBase)  
    **Returns:**  A score between 0 and 1 (higher is more similar).


##### Description
Jaccard similarity is a measure of similarity between two sets of items, based on the size of their intersection (elements in common) and union (total elements across both sets). For strings, it considers the overlap of characters within each string. Mathematically, it can be represented as:

$$Jaccard=\frac{|A \cap B|}{|A \cup B|}$$

where A and B are two strings, and |A| and |B| represent their cardinalities (i.e., the number of elements in each set).

In practice, Jaccard is more useful with strings that can be split up into multiple words as opposed to characters within a single word or string. E.g. tokens within addresses:

Address1: {"flat", "2", "123", "high", "street", "london", "sw1", "1ab"}  
Address2: {"2", "high", "street", "london", "sw1a", "1ab"}

Where 

- There are 9 unique tokens across the addresses: "flat", "2", "123", "high", "street", "london", "sw1", "sw1a", "1ab"  
- There are 5 tokens found in both addresses: "2", "high", "street", "london", "1ab"

We calculate the Jaccard similarity using the formula:

$$Jaccard(Address1, Address2)=\frac{5}{9}=0.5556$$

However, this functionality is not curently implemented within Splink

##### Examples 

??? example  "**"DUCK" vs "LUCK"**"

    - There are five unique characters across the strings: "D", "U", "C", "K", "L"
    - Three are found in both strings: "U", "C", "K"

    We calculate the Jaccard similarity using the formula:

    $$Jaccard(DUCK, LUCK)=\frac{3}{5}=0.6$$

??? example "**"MARTHA" vs "MARHTA"**"

    - There are five unique characters across the strings: "M", "A", "R", "T", "H"
    - Five are found in both strings: "M", "A", "R", "T", "H"

    We calculate the Jaccard similarity using the formula:

    $$Jaccard(MARTHA, MARHTA)=\frac{5}{5}=1$$


##### Sample code

You can test out the Jaccard similarity between two strings with the function below:

```python
def jaccard_similarity(str1, str2):
        set1 = set(str1)
        set2 = set(str2)
        return len(set1 & set2) / len(set1 | set2)

jaccard_similarity("DUCK", "LUCK")
```
> 0.6

<hr>
