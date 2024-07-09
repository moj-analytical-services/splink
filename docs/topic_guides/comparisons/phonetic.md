---
tags:
  - API
  - Phonetic Transformations
  - Comparisons
  - Blocking
  - Soundex
  - Metaphone
  - Double Metaphone
---


# Phonetic transformation algorithms

Phonetic transformation algorithms can be used to identify words that sound similar, even if they are spelled differently (e.g. "Stephen" vs "Steven"). These algorithms to give another type of fuzzy match and are often generated in the [Feature Engineering](../data_preparation/feature_engineering.md#phonetic-transformations) step of record linkage.

Once generated, phonetic matches can be used within [comparisons & comparison levels](../comparisons//customising_comparisons.ipynb) and [blocking rules](../blocking/blocking_rules.md).


```python
import splink.comparison_library as cl

first_name_comparison = cl.NameComparison(
                        "first_name",
                        dmeta_col_name= "first_name_dm")
print(first_name_comparison.human_readable_description)
```

```
Comparison 'NameComparison' of "first_name" and "first_name_dm".
Similarity is assessed using the following ComparisonLevels:

    - 'first_name is NULL' with SQL rule: "first_name_l" IS NULL OR "first_name_r" IS NULL
    - 'Exact match on first_name' with SQL rule: "first_name_l" = "first_name_r"
    - 'Jaro-Winkler distance of first_name >= 0.92' with SQL rule: jaro_winkler_similarity("first_name_l", "first_name_r") >= 0.92
    - 'Jaro-Winkler distance of first_name >= 0.88' with SQL rule: jaro_winkler_similarity("first_name_l", "first_name_r") >= 0.88
    - 'Array intersection size >= 1' with SQL rule: array_length(list_intersect("first_name_dm_l", "first_name_dm_r")) >= 1
    - 'Jaro-Winkler distance of first_name >= 0.7' with SQL rule: jaro_winkler_similarity("first_name_l", "first_name_r") >= 0.7
    - 'All other comparisons' with SQL rule: ELSE
```


<hr>

## Algorithms

Below are some examples of well known phonetic transformation algorithms.

### Soundex

Soundex is a phonetic algorithm that assigns a code to words based on their sound. The Soundex algorithm works by converting a word into a four-character code, where the first character is the first letter of the word, and the next three characters are numerical codes representing the word's remaining consonants. Vowels and some consonants, such as H, W, and Y, are ignored.

??? info "Algorithm Steps"

    The Soundex algorithm works by following these steps:

    1. Retain the first letter of the word and remove all other vowels and the letters "H", "W", and "Y".

    2. Replace each remaining consonant (excluding the first letter) with a numerical code as follows:
        1. B, F, P, and V are replaced with "1"
        2. C, G, J, K, Q, S, X, and Z are replaced with "2"
        3. D and T are replaced with "3"
        4. L is replaced with "4"
        5. M and N are replaced with "5"
        6. R is replaced with "6"

    3. Combine the first letter and the numerical codes to form a four-character code. If there are fewer than four characters, pad the code with zeros.

???+ example

    You can test out the Soundex transformation between two strings through the [phonetics](https://github.com/ZackDibe/phonetics) package.

    ```python
    import phonetics
    print(phonetics.soundex("Smith"), phonetics.soundex("Smyth"))
    ```
    > S5030 S5030


<hr>

### Metaphone
Metaphone is an improved version of the Soundex algorithm that was developed to handle a wider range of words and languages. The Metaphone algorithm assigns a code to a word based on its phonetic pronunciation, but it takes into account the sound of the entire word, rather than just its first letter and consonants.
The Metaphone algorithm works by applying a set of rules to the word's pronunciation, such as converting the "TH" sound to a "T" sound, or removing silent letters. The resulting code is a variable-length string of letters that represents the word's pronunciation.

??? info "Algorithm Steps"

    The Metaphone algorithm works by following these steps:

    1. Convert the word to uppercase and remove all non-alphabetic characters.

    2. Apply a set of pronunciation rules to the word, such as:
        1. Convert the letters "C" and "K" to "K"
        2. Convert the letters "PH" to "F"
        3. Convert the letters "W" and "H" to nothing if they are not at the beginning of the word

    3. Apply a set of replacement rules to the resulting word, such as:
        1. Replace the letter "G" with "J" if it is followed by an "E", "I", or "Y"
        2. Replace the letter "C" with "S" if it is followed by an "E", "I", or "Y"
        3. Replace the letter "X" with "KS"

    4. If the resulting word ends with "S", remove it.

    5. If the resulting word ends with "ED", "ING", or "ES", remove it.

    6. If the resulting word starts with "KN", "GN", "PN", "AE", "WR", or "WH", remove the first letter.

    7. If the resulting word starts with a vowel, retain the first letter.

    8. Retain the first four characters of the resulting word, or pad it with zeros if it has fewer than four characters.

???+ example

    You can test out the Metaphone transformation between two strings through the [phonetics](https://github.com/ZackDibe/phonetics) package.

    ```python
    import phonetics
    print(phonetics.metaphone("Smith"), phonetics.metaphone("Smyth"))
    ```
    > SM0 SM0


<hr>

### Double Metaphone
Double Metaphone is an extension of the Metaphone algorithm that generates two codes for each word, one for the primary pronunciation and one for an alternate pronunciation. The Double Metaphone algorithm is designed to handle a wide range of languages and dialects, and it is more accurate than the original Metaphone algorithm.

The Double Metaphone algorithm works by applying a set of rules to the word's pronunciation, similar to the Metaphone algorithm, but it generates two codes for each word. The primary code is the most likely pronunciation of the word, while the alternate code represents a less common pronunciation.

??? info "Algorithm Steps"
    === "Standard Double Metaphone"
        The Double Metaphone algorithm works by following these steps:

        1. Convert the word to uppercase and remove all non-alphabetic characters.

        2. Apply a set of pronunciation rules to the word, such as:
            1. Convert the letters "C" and "K" to "K"
            2. Convert the letters "PH" to "F"
            3. Convert the letters "W" and "H" to nothing if they are not at the beginning of the word

        3. Apply a set of replacement rules to the resulting word, such as:
            1. Replace the letter "G" with "J" if it is followed by an "E", "I", or "Y"
            2. Replace the letter "C" with "S" if it is followed by an "E", "I", or "Y"
            3. Replace the letter "X" with "KS"

        4. If the resulting word ends with "S", remove it.

        5. If the resulting word ends with "ED", "ING", or "ES", remove it.

        6. If the resulting word starts with "KN", "GN", "PN", "AE", "WR", or "WH", remove the first letter.

        7. If the resulting word starts with "X", "Z", "GN", or "KN", retain the first two characters.

        8. Apply a second set of rules to the resulting word to generate an alternative code.

        9. Return the primary and alternative codes as a tuple.

    === "Alternative Double Metaphone"

        The Alternative Double Metaphone algorithm takes into account different contexts in the word and is generated by following these steps:

        1. Apply a set of prefix rules, such as:
            1. Convert the letter "G" at the beginning of the word to "K" if it is followed by "N", "NED", or "NER"
            2. Convert the letter "A" at the beginning of the word to "E" if it is followed by "SCH"

        2. Apply a set of suffix rules, such as:
            1. Convert the letters "E" and "I" at the end of the word to "Y"
            2. Convert the letters "S" and "Z" at the end of the word to "X"
            3. Remove the letter "D" at the end of the word if it is preceded by "N"

        3. Apply a set of replacement rules, such as:
            1. Replace the letter "C" with "X" if it is followed by "IA" or "H"
            2. Replace the letter "T" with "X" if it is followed by "IA" or "CH"

        4. Retain the first four characters of the resulting word, or pad it with zeros if it has fewer than four characters.

        5. If the resulting word starts with "X", "Z", "GN", or "KN", retain the first two characters.

        6. Return the alternative code.

???+ example

    You can test out the Metaphone transformation between two strings through the [phonetics](https://github.com/ZackDibe/phonetics) package.

    ```python
    import phonetics
    print(phonetics.dmetaphone("Smith"), phonetics.dmetaphone("Smyth"))
    ```
    > ('SM0', 'XMT') ('SM0', 'XMT')

<hr>
