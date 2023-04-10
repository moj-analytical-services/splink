---
tags:
  - API
  - Feature Engineering
  - Comparisons
  - Blocking
  - Soundex
  - Double Metaphone
---

# Feature Engineering for Data Linkage

## Full Name

### Example

## Postcodes

Many datasets contain postcode data and there are a number of ways to compare the postcode strings.

For example, levenshtein distances are useful to spot any typos/character swaps:

```
import splink.duckdb.duckdb_comparison_library as cl

postcode_comparison = cl.levenshtein_at_thresholds("postcode", [2])
```

However, string comparators alone don't necessarily give the best sense of whether two postcodes are similar. Another way of considering similarity of postcodes is the physical distance between them. Splink has a functions to calculate the distance between two sets of coordinates ([cll.distance_in_KM_level()](../comparison_level_library.md#splink.comparison_level_library.DistanceFunctionLevelBase) and [cl.distance_in_KM_at_thresholds()](../comparison_library.md#splink.comparison_library.DistanceInKMAtThresholdsComparisonBase)) which can be utilised, alongside string comparisons, to give better results.

### Example

There are a number of open source repositories of geospatial data that can be used for linkage, one example is [geonames](http://download.geonames.org/export/zip/). 

Below is an example of adding lattitude and longitude columns from geonames to create a more nuanced comparison.

Read in a dataset with postcodes:

```
import pandas as pd

df = pd.read_parquet("/PATH/TO/DEMO/DATA/historical_figures_with_errors_50k.parquet")
df["postcode_fake"] = df["postcode_fake"].str.upper()
df.head()
```
 
|    | unique_id   | cluster   | full_name                                        | first_and_surname   | first_name   | surname   | dob        | birth_place   | postcode_fake   | gender   | occupation   |
|---:|:------------|:----------|:-------------------------------------------------|:--------------------|:-------------|:----------|:-----------|:--------------|:----------------|:---------|:-------------|
|  0 | Q2296770-1  | Q2296770  | thomas clifford, 1st baron clifford of chudleigh | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF        | male     | politician   |
|  1 | Q2296770-2  | Q2296770  | thomas of chudleigh                              | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF        | male     | politician   |
|  2 | Q2296770-3  | Q2296770  | tom 1st baron clifford of chudleigh              | tom chudleigh       | tom          | chudleigh | 1630-08-01 | devon         | TQ13 8DF        | male     | politician   |
|  3 | Q2296770-4  | Q2296770  | thomas 1st chudleigh                             | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8HU        |          | politician   |
|  4 | Q2296770-5  | Q2296770  | thomas clifford, 1st baron chudleigh             | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF        |          | politician   |

Then read in a list of GB postcodes downloaded from [geonames](http://download.geonames.org/export/zip/).

```
import pandas as pd

names = ['dataset', 'postcode', 'area', 'country', 'country_code', 'region', 'unknown', 'region2', 'region3', 'latitude', 'longitude','feature code']
postcodes = pd.read_csv("GB_full.txt", sep="\t", header = None, names=names)
postcodes.head(5)
```

|    | dataset   | postcode   | area       | country   | country_code   | region        |   unknown | region2                 | region3   |   latitude |   longitude |   feature code |
|---:|:----------|:-----------|:-----------|:----------|:---------------|:--------------|----------:|:------------------------|:----------|-----------:|------------:|---------------:|
|  0 | GB        | AL3 8QE    | Slip End   | England   | ENG            | Bedfordshire  |       nan | Central Bedfordshire    | E06000056 |    51.8479 |     -0.4474 |              6 |
|  1 | GB        | AL5 3NG    | Harpenden  | England   | ENG            | Bedfordshire  |       nan | Central Bedfordshire    | E06000056 |    51.8321 |     -0.383  |              6 |
|  2 | GB        | AL5 3NS    | Hyde       | England   | ENG            | Bedfordshire  |       nan | Central Bedfordshire    | E06000056 |    51.8333 |     -0.3763 |              6 |
|  3 | GB        | AL5 3QF    | Hyde       | England   | ENG            | Bedfordshire  |       nan | Central Bedfordshire    | E06000056 |    51.8342 |     -0.3851 |              6 |
|  4 | GB        | B10 0AB    | Birmingham | England   | ENG            | West Midlands |       nan | Birmingham District (B) | E08000025 |    52.4706 |     -1.875  |              6 |

Now combine the lat-long coordinates from the `GB_full.txt` lookup.

```
df_with_coordinates = df.merge(postcodes[["postcode", "latitude", "longitude"]], 
                                    left_on="postcode_fake", 
                                    right_on="postcode",
                                    how="left")

df_with_coordinates.head()
```

|    | unique_id   | cluster   | full_name                                        | first_and_surname   | first_name   | surname   | dob        | birth_place   | postcode_fake   | gender   | occupation   | postcode   |   latitude |   longitude |
|---:|:------------|:----------|:-------------------------------------------------|:--------------------|:-------------|:----------|:-----------|:--------------|:----------------|:---------|:-------------|:-----------|-----------:|------------:|
|  0 | Q2296770-1  | Q2296770  | thomas clifford, 1st baron clifford of chudleigh | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF        | male     | politician   | TQ13 8DF   |    50.6927 |     -3.8139 |
|  1 | Q2296770-2  | Q2296770  | thomas of chudleigh                              | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF        | male     | politician   | TQ13 8DF   |    50.6927 |     -3.8139 |
|  2 | Q2296770-3  | Q2296770  | tom 1st baron clifford of chudleigh              | tom chudleigh       | tom          | chudleigh | 1630-08-01 | devon         | TQ13 8DF        | male     | politician   | TQ13 8DF   |    50.6927 |     -3.8139 |
|  3 | Q2296770-4  | Q2296770  | thomas 1st chudleigh                             | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8HU        |          | politician   | TQ13 8HU   |    50.6876 |     -3.8958 |
|  4 | Q2296770-5  | Q2296770  | thomas clifford, 1st baron chudleigh             | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF        |          | politician   | TQ13 8DF   |    50.6927 |     -3.8139 |

Now that coordinates have been added, a more detailed postcode comparison can be created to be included in a splink settings dictionary:

```
import splink.duckdb.duckdb_comparison_level_library as cll

postcode_comparison = {
    'output_column_name': 'postcode',
    'comparison_description': 'Postcode',
    'comparison_levels': [
        cll.null_level("postcode"),
        cll.exact_match_level("postcode", term_frequency_adjustments=True),
        cll.levenshtein_level("postcode", 1),
        cll.distance_in_km_level("lattitude", "longitude", 1),
        cll.distance_in_km_level("lattitude", "longitude", 10),
        cll.distance_in_km_level("lattitude", "longitude", 50),
        cll.else_level()
    ],
}
```


## Phonetic transformations

Phonetic transformation algorithms can be used to identify words that sound similar, even if they are spelled differently. These are particularly useful for names. 

For blocking rules, it allows for possible candidate pairs of entities with phonetically similar transforms to be considered for linking. This can result in a "fuzzier" [blocking process](/blocking_rules.md#the-purpose-of-blocking_rules_to_generate_predictions), which may be beneficial in certain instances.

Similarly, phonetically similar transforms offer another way to do fuzzy-matching within a [comparison](/customising_comparisons.ipynb).

Below are some examples of well known phonetic transformation algorithmns.

There are a number of ways you can generate phonetic transformation columns within a dataset. Below are a few examples of how this can be achieved.

### Example

There are a number of python packages which support phonetic transformations that can be applied to a pandas dataframe, which can then be loaded into the DuckDBLinker. For example, using the [phonetics](https://pypi.org/project/phonetics/) python library:

```
import pandas as pd
import phonetics

df = pd.read_parquet("PATH/TO/DATA/fake_1000.parquet")

# Define a function to apply the dmetaphone phonetic algorithm to each name in the column
def dmetaphone_name(name):
    if name is None:
        pass
    else:
        return phonetics.dmetaphone(name)[0]

# Apply the function to the "first_name" and surname columns using the apply method
df['first_name_dm'] = df['first_name'].apply(dmetaphone_name)
df['surname_dm'] = df['surname'].apply(dmetaphone_name)

df.head(7)
```

|unique_id|first_name|surname|dob|city|email|group|first_name_dm|surname_dm|
|---------|----------|-------|---|----|-----|-----|-------------|----------|
|0	|Julia	|None	|2015-10-29	|London	|hannah88@powers.com	|0	|JL	|None|
|1	|Julia	|Taylor	|2015-07-31	|London	|hannah88@powers.com	|0	|JL	|TLR|
|2	|Julia	|Taylor	|2016-01-27	|London	|hannah88@powers.com	|0	|JL	|TLR|
|3	|Julia	|Taylor	|2015-10-29	|None	|hannah88opowersc@m	|0	|JL	|TLR|
|4	|oNah	|Watson	|2008-03-23	|Bolton	|matthew78@ballard-mcdonald.net	|1	|AN	|ATSN|

Now that the dmetaphone columns have been added, they can be used within comparisons. For example, using the [name_comparison](../comparison_template_library.md#splink.comparison_template_library.NameComparisonBase) function from the [comparison template library](customising_comparisons.ipynb#name-comparisons)

```
import splink.duckdb.duckdb_comparison_template_library as ctl

first_name_comparison = ctl.name_comparison(
                            "first_name",
                            phonetic_col_name = "first_name_dm")
print(first_name_comparison.human_readable_description)
```


> Comparison 'Exact match vs. Names with phonetic exact match vs. Names within jaro_winkler thresholds 0.95, 0.88 vs. anything else' of "first_name" and "first_name_dm".
>
> Similarity is assessed using the following ComparisonLevels:
>
>    - 'Null' with SQL rule: "first_name_l" IS NULL OR "first_name_r" IS NULL
>    - 'Exact match first_name' with SQL rule: "first_name_l" = "first_name_r"
>    - 'Exact match first_name_dm' with SQL rule: "first_name_dm_l" = "first_name_dm_r"
>    - 'Jaro_winkler_similarity >= 0.95' with SQL rule: jaro_winkler_similarity("first_name_l", "first_name_r") >= 0.95
>    - 'Jaro_winkler_similarity >= 0.88' with SQL rule: jaro_winkler_similarity("first_name_l", "first_name_r") >= 0.88
>    - 'All other comparisons' with SQL rule: ELSE


## Appendix

### Phonetic Transformation Algorithm In Detail
#### Soundex

Soundex is a phonetic algorithm that assigns a code to words based on their sound. The Soundex algorithm works by converting a word into a four-character code, where the first character is the first letter of the word, and the next three characters are numerical codes representing the word's remaining consonants. Vowels and some consonants, such as H, W, and Y, are ignored.

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

For example, the Soundex code for the name "Smith" is S530, and the code for "Smyth" is also S530. This allows for similar-sounding names to be indexed and searched together.



#### Metaphone
Metaphone is an improved version of the Soundex algorithm that was developed to handle a wider range of words and languages. The Metaphone algorithm assigns a code to a word based on its phonetic pronunciation, but it takes into account the sound of the entire word, rather than just its first letter and consonants.
The Metaphone algorithm works by applying a set of rules to the word's pronunciation, such as converting the "TH" sound to a "T" sound, or removing silent letters. The resulting code is a variable-length string of letters that represents the word's pronunciation.

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

For example, the Metaphone code for the name "Smith" is SM0, and the code for "Smyth" is also SM0. This allows for more accurate indexing and searching of similar-sounding words.

#### Double Metaphone
Double Metaphone is an extension of the Metaphone algorithm that generates two codes for each word, one for the primary pronunciation and one for an alternate pronunciation. The Double Metaphone algorithm is designed to handle a wide range of languages and dialects, and it is more accurate than the original Metaphone algorithm.

The Double Metaphone algorithm works by applying a set of rules to the word's pronunciation, similar to the Metaphone algorithm, but it generates two codes for each word. The primary code is the most likely pronunciation of the word, while the alternate code represents a less common pronunciation.

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

The second set of rules for the alternative code is similar to the first set, but it takes into account different contexts in the word. The alternative code is generated by following these steps:

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

For example, the Double Metaphone code for the name "Smith" is SM0 and XMT, while the code for "Smyth" is also SM0 and XMT. This allows for even more accurate indexing and searching of similar-sounding words.

