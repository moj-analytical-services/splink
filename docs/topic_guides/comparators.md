---
tags:
  - API
  - comparisons
  - Levenstein
  - Jaro-Winkler
  - Jaccard
---
# Comparators


String comparators are algorithms used in data linkage to compare and evaluate the similarity between two strings of text. These algorithms can be applied to different types of data, including names, addresses, and other identifying information, to determine if two records refer to the same individual or entity.

One common use of string comparators in data linkage is to identify duplicates within a dataset. By comparing the strings of two records and calculating their similarity, the algorithm can flag records that may be duplicates and require further investigation. This can help to improve the accuracy and completeness of the data by eliminating incorrect or redundant information.

Another use of string comparators in data linkage is to merge different datasets or databases. By comparing strings across multiple sources, the algorithm can identify records that refer to the same individual or entity and merge them into a single record. This can be useful for creating a more comprehensive and accurate view of the data, and for identifying trends and patterns that may not be visible in a single dataset.


## Jaro similarity


The Jaro similarity measure is a string similarity measure that quantifies the similarity between two strings based on the number of common characters between them and their relative position. It is often used in applications such as entity resolution and duplicate detection, and is particularly useful for comparing strings that are similar in length and contain similar characters.

The formula for Jaro similarity is as follows:

$$Jaro = \frac{1}{3} \left[ \frac{m}{|s_1|} + \frac{m}{|s_2|} + \frac{m-t}{m} \right]$$

where $s_1$ and $s_2$ are the two strings being compared, 

$m$ is the number of common characters (which are considered matching only if they are the same and not farther than $\left\lfloor \frac{\min(|s_1|,|s_2|)}{2} \right\rfloor - 1$ characters apart), 

and $t$ is the number of transpositions (which is calculated as the number of matching characters that are not in the right order divided by two).

Jaro similarity is not currently supported in splink. If you think this would be useful, please let us know, or contribute it yourself, on [github.](https://github.com/moj-analytical-services/splink/issues/1107)


## Jaro Winkler similarity


The formula for Jaro Winkler is:

$$Jaro Winkler = Jaro + p \cdot l \cdot (1 - Jaro)$$

where :

$Jaro$ is the Jaro similarity score, 
$p$ is the scaling factorfor how much the score is adjusted upwards for having common prefixes.$p$ should not exceed 0.25 
(i.e. 1/4, with 4 being the maximum length of the prefix being considered), otherwise the similarity could become larger than 1. The standard value for this constant in Winkler's work is $p$ = 0.1 )

$l$ is the length of the common prefix, 
and $1 - Jaro$ is the penalty for non-matching characters at the beginning of the strings.

See the [comparison library](../comparison_library.md#splink.comparison_library.JaroWinklerAtThresholdsComparisonBase) and [comparison level library](../comparison_level_library.md#splink.comparison_level_library.JaroWinklerLevelBase) docs for the Jaro Winkler functions in splink.

## Levenstein edit distance 

The Levenstein edit distance is a measure of the similarity between two strings of text. It is calculated by counting the number of operations (insertions, deletions, or substitutions) needed to transform one string into the other.


The formula for Levenstein Distance, also known as Edit Distance, is:

$$Levenstein(s_1, s_2) = \min \lbrace \begin{array}{l}
\text{insertion} \ ,
\text{deletion} ,
\text{substitution} 
\end{array} \rbrace $$

where $s_1$ and $s_2$ are the two strings being compared. This metric measures the minimum number of edit operations (insertions, deletions, and substitutions) required to transform one string into the other, and can be used to compare strings that may not be identical but are still similar.

See the [comparison library](../comparison_library.md#splink.comparison_library.LevenshteinAtThresholdsComparisonBase) and [comparison level library](../comparison_level_library.md#splink.comparison_level_library.LevenshteinLevelBase) docs for the Levenshtein functions in splink.

## Jaccard similarity 


The Jaccard similarity is a measure of similarity between two sets of data, and is often used in text analysis to compare the similarity of two string/documents.

The Jaccard similarity formula is:

$$J(A,B)=\frac{|A \cap B|}{|A \cup B|}$$

In order for the algorith to work it is necessary to first divide each string into "shingles." Shingles refer to fixed-size subsets of a given set of data. 

For example, if we have a field containing a long string of words such as an address or a company name, the algorithm divides it into shingles of a fixed size (e.g. one word per shingle or perhaps a trigram). This creates a set of shingles from the original data, which allows us to efficiently calculate the Jaccard similarity between it and other sets of data by comparing the shingles they share

See the [comparison library](../comparison_library.md#splink.comparison_library.JaccardAtThresholdsComparisonBase) and [comparison level library](../comparison_level_library.md#splink.comparison_level_library.JaccardLevelBase) docs for the Jaccard functions in splink.