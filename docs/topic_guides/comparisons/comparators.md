---
tags:
  - API
  - comparisons
  - Levenshtein
  - Damerau-Levenshtein
  - Jaro
  - Jaro-Winkler
  - Jaccard
---
# String Comparators


There are a number of string comparator functions available in Splink that allow fuzzy matching for strings within [`Comparisons`](../comparisons/comparisons_and_comparison_levels.md) and [`Comparison Levels`](../comparisons/comparisons_and_comparison_levels.md). For each of these fuzzy matching functions, below you will find explanations of how they work, worked examples and recommendations for the types of data they are useful for.

For guidance on how to choose the most suitable string comparator, and associated threshold, see the dedicated [topic guide](./choosing_comparators.ipynb).



<hr>

## Levenshtein Distance

!!! info "At a glance"
    **Useful for:** Data entry errors e.g. character miskeys.</br>
    **Splink comparison functions:** [levenshtein_level()](../../api_docs/comparison_level_library.md#splink.comparison_level_library.LevenshteinLevel) and [levenshtein_at_thresholds()](../../api_docs/comparison_library.md#splink.comparison_library.LevenshteinAtThresholds)</br>
    **Returns:** An integer (lower is more similar).</br>

##### Description
Levenshtein distance, also known as edit distance, is a measure of the difference between two strings. It represents the minimum number of **insertions**, **deletions**, or **substitutions** of characters required to transform one string into the other.

Or, as a formula,

$$\textsf{Levenshtein}(s_1, s_2) = \min \lbrace \begin{array}{l}
\text{insertion , }
\text{deletion , }
\text{substitution}
\end{array} \rbrace $$

##### Examples

??? example "**"KITTEN" vs "SITTING"**"

    The minimum number of operations to convert "KITTEN" into "SITTING" are:

    - Substitute "K" in "KITTEN" with "S" to get "SITTEN."
    - Substitute "E" in "SITTEN" with "I" to get "SITTIN."
    - Insert "G" after "N" in "SITTIN" to get "SITTING."

    Therefore,

    $$\textsf{Levenshtein}(\texttt{KITTEN}, \texttt{SITTING}) = 3$$

??? example "**"CAKE" vs "ACKE"**"

    The minimum number of operations to convert "CAKE" into "ACKE" are:

    - Substitute "C" in "CAKE" with "A" to get "AAKE."
    - substitute the second "A" in "AAKE" with "C" to get "ACKE."

    Therefore,

    $$\textsf{Levenshtein}(\texttt{CAKE}, \texttt{ACKE}) = 2$$


##### Sample code

You can test out the Levenshtein distance as follows:

```python
import duckdb
duckdb.sql("SELECT levenshtein('CAKE', 'ACKE')").df().iloc[0,0]
```
> 2

<hr>

## Damerau-Levenshtein Distance

!!! info "At a glance"
    **Useful for:** Data entry errors e.g. character transpositions and miskeys</br>
    **Splink comparison functions:** [damerau_levenshtein_level()](../../api_docs/comparison_level_library.md#splink.comparison_level_library.DamerauLevenshteinLevel) and [damerau_levenshtein_at_thresholds()](../../api_docs/comparison_library.md#splink.comparison_library.DamerauLevenshteinAtThresholds)</br>
    **Returns:** An integer (lower is more similar).</br>

##### Description
Damerau-Levenshtein distance is a variation of [Levenshtein distance](#levenshtein-distance) that also includes transposition operations, which are the interchange of adjacent characters. This distance measures the minimum number of operations required to transform one string into another by allowing **insertions**, **deletions**, **substitutions**, and **transpositions** of characters.

Or, as a formula,

$$\textsf{DamerauLevenshtein}(s_1, s_2) = \min \lbrace \begin{array}{l}
\text{insertion , }
\text{deletion , }
\text{substitution , }
\text{transposition}
\end{array} \rbrace $$

##### Examples

??? example "**"KITTEN" vs "SITTING"**"

    The minimum number of operations to convert "KITTEN" into "SITTING" are:

    - Substitute "K" in "KITTEN" with "S" to get "SITTEN".
    - Substitute "E" in "SITTEN" with "I" to get "SITTIN".
    - Insert "G" after "T" in "SITTIN" to get "SITTING".

    Therefore,

    $$\textsf{DamerauLevenshtein}(\texttt{KITTEN}, \texttt{SITTING}) = 3$$

??? example "**"CAKE" vs "ACKE"**"

    The minimum number of operations to convert "CAKE" into "ACKE" are:

    - Transpose "C" and "A" in "CAKE" with "A" to get "ACKE."

    Therefore,

    $$\textsf{DamerauLevenshtein}(\texttt{CAKE}, \texttt{ACKE}) = 1$$

##### Sample code

You can test out the Damerau-Levenshtein distance as follows:

```python
import duckdb
duckdb.sql("SELECT damerau_levenshtein('CAKE', 'ACKE')").df().iloc[0,0]
```
> 1

<hr>


## Jaro Similarity

!!! info "At a glance"
    **Useful for:**  Strings where all characters are considered equally important, regardless of order e.g. ID numbers</br>
    **Splink comparison functions:**  [jaro_level()](../../api_docs/comparison_level_library.md#splink.comparison_level_library.JaroLevel) and [jaro_at_thresholds()](../../api_docs/comparison_library.md#splink.comparison_library.JaroAtThresholds)</br>
    **Returns:**  A score between 0 and 1 (higher is more similar)</br>

##### Description
Jaro similarity is a measure of similarity between two strings. It takes into account the number and order of matching characters, as well as the number of transpositions needed to make the strings identical.

Jaro similarity considers:

- The number of matching characters (characters in the same position in both strings).
- The number of transpositions (pairs of characters that are not in the same position in both strings).

Or, as a formula:

$$\textsf{Jaro}(s_1, s_2) = \frac{1}{3} \left[ \frac{m}{|s_1|} + \frac{m}{|s_2|} + \frac{m-t}{m} \right]$$

where:

- $s_1$ and $s_2$ are the two strings being compared
- $m$ is the number of common characters (which are considered matching only if they are the same and not farther than $\left\lfloor \frac{\min(|s_1|,|s_2|)}{2} \right\rfloor - 1$ characters apart)
- $t$ is the number of transpositions (which is calculated as the number of matching characters that are not in the right order divided by two).

##### Examples

??? example "**"MARTHA" vs "MARHTA":**"

    - There are four matching characters: "M", "A", "R", and "T".
    - There is one transposition: the fifth character in "MARTHA" ("H") is not in the same position as the fifth character in "MARHTA" ("T").
    - We calculate the Jaro similarity using the formula:

    $$\textsf{Jaro}(\texttt{MARTHA}, \texttt{MARHTA}) = \frac{1}{3} \left[ \frac{4}{6} + \frac{4}{6} + \frac{4-1}{4} \right] = 0.944$$

??? example "**"MARTHA" vs "AMRTHA":**"

    - There are four matching characters: "M", "A", "R", and "T".
    - There is one transposition: the first character in "MARTHA" ("M") is not in the same position as the first character in "AMRTHA" ("T").
    - We calculate the Jaro similarity using the formula:

    $$\textsf{Jaro}(\texttt{MARTHA}, \texttt{AMRTHA}) = \frac{1}{3} \left[ \frac{4}{6} + \frac{4}{6} + \frac{4-1}{4} \right] = 0.944$$

    Noting that transpositions of strings gives the same Jaro similarity regardless of order.

##### Sample code

You can test out the Jaro similarity as follows:

```python
import duckdb
duckdb.sql("SELECT jaro_similarity('MARTHA', 'MARHTA')").df().iloc[0,0]
```
> 0.944

<hr>

## Jaro-Winkler Similarity

!!! info "At a glance"
    **Useful for:** Strings where importance is weighted towards the first 4 characters e.g. Names</br>
    **Splink comparison functions:** [jaro_winkler_level()](../../api_docs/comparison_level_library.md#splink.comparison_level_library.JaroWinklerLevel) and [jaro_winkler_at_thresholds()](../../api_docs/comparison_library.md#splink.comparison_library.JaroWinklerAtThresholds)</br>
    **Returns:**  A score between 0 and 1 (higher is more similar).</br>


##### Description
Jaro-Winkler similarity is a variation of [Jaro similarity](#jaro-similarity) that gives extra weight to matching prefixes of the strings. It is particularly useful for names

The Jaro-Winkler similarity is calculated as follows:

$$\textsf{JaroWinkler}(s_1, s_2) = \textsf{Jaro}(s_1, s_2) + p \cdot l \cdot (1 - \textsf{Jaro}(s_1, s_2))$$

where:
- $\textsf{Jaro}(s_1, s_2)$ is the [Jaro similarity](#jaro-similarity) between the two strings
- $l$ is the length of the common prefix between the two strings, up to a maximum of four characters
- $p$ is a prefix scale factor, commonly set to 0.1.

##### Examples

??? example  "**"MARTHA" vs "MARHTA"**"

    The common prefix between the two strings is "MAR", which has a length of 3.
    We calculate the Jaro-Winkler similarity using the formula:

    $$\textsf{Jaro-Winkler}(\texttt{MARTHA}, \texttt{MARHTA}) = 0.944 + 0.1 \cdot 3 \cdot (1 - 0.944) = 0.9612$$

    The Jaro-Winkler similarity is slightly higher than the Jaro similarity, due to the matching prefix.

??? example  "**"MARTHA" vs "AMRTHA":**"

    There is no common prefix, so the Jaro-Winkler similarity formula gives:

    $$\textsf{Jaro-Winkler}(\texttt{MARTHA}, \texttt{MARHTA}) = 0.944 + 0.1 \cdot 0 \cdot (1 - 0.944) = 0.944$$

    Which is the same as the Jaro score.

    Note that the Jaro-Winkler similarity should be used with caution, as it may not always provide better results than the standard Jaro similarity, especially when dealing with short strings or strings that have no common prefix.

##### Sample code

You can test out the Jaro similarity as follows:

```python
import duckdb
duckdb.sql("SELECT jaro_winkler_similarity('MARTHA', 'MARHTA')").df().iloc[0,0]
```
> 0.9612

<hr>

## Jaccard Similarity

!!! info "At a glance"
    **Useful for:**</br>
    **Splink comparison functions:** [jaccard_level()](../../api_docs/comparison_level_library.md#splink.comparison_level_library.JaccardLevel) and [jaccard_at_thresholds()]</br>(../../comparison_library.md#splink.comparison_library.JaccardAtThresholdsBase)
    **Returns:**  A score between 0 and 1 (higher is more similar).</br>


##### Description
Jaccard similarity is a measure of similarity between two sets of items, based on the size of their intersection (elements in common) and union (total elements across both sets). For strings, it considers the overlap of characters within each string. Mathematically, it can be represented as:

$$\textsf{Jaccard}=\frac{|A \cap B|}{|A \cup B|}$$

where A and B are two strings, and |A| and |B| represent their cardinalities (i.e., the number of elements in each set).

In practice, Jaccard is more useful with strings that can be split up into multiple words as opposed to characters within a single word or string. E.g. tokens within addresses:

**Address 1**: {"flat", "2", "123", "high", "street", "london", "sw1", "1ab"}

**Address 2**: {"2", "high", "street", "london", "sw1a", "1ab"},

where:

- there are 9 unique tokens across the addresses: "flat", "2", "123", "high", "street", "london", "sw1", "sw1a", "1ab"
- there are 5 tokens found in both addresses: "2", "high", "street", "london", "1ab"

We calculate the Jaccard similarity using the formula:

$$\textsf{Jaccard}(\textrm{Address1}, \textrm{Address2})=\frac{5}{9}=0.5556$$

However, this functionality is not currently implemented within Splink

##### Examples

??? example  "**"DUCK" vs "LUCK"**"

    - There are five unique characters across the strings: "D", "U", "C", "K", "L"
    - Three are found in both strings: "U", "C", "K"

    We calculate the Jaccard similarity using the formula:

    $$\textsf{Jaccard}(\texttt{DUCK}, \texttt{LUCK})=\frac{3}{5}=0.6$$

??? example "**"MARTHA" vs "MARHTA"**"

    - There are five unique characters across the strings: "M", "A", "R", "T", "H"
    - Five are found in both strings: "M", "A", "R", "T", "H"

    We calculate the Jaccard similarity using the formula:

    $$\textsf{Jaccard}(\texttt{MARTHA}, \texttt{MARHTA})=\frac{5}{5}=1$$


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
