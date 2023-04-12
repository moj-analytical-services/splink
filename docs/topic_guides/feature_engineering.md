---
tags:
  - API
  - Feature Engineering
  - Comparisons
  - Postcode
  - Phonetic Transformations
  - Soundex
  - Metaphone
  - Double Metaphone
---

# Feature Engineering for Data Linkage

During record linkage, the features in a given dataset are used to provide evidence as to whether two records are a match. Like any predictive model, the quality of a Splink model is dictated by the features provided. 

Below are some examples of features that be created from common columns, and how to create more detailed comparisons with them in a Splink model.

## Postcodes

Many datasets contain postcode data and there are a number of ways to compare the postcode strings.

For example, levenshtein distances are useful to spot any typos/character swaps:

```python
import splink.duckdb.duckdb_comparison_library as cl

postcode_comparison = cl.levenshtein_at_thresholds("postcode", [2])
print(postcode_comparison.human_readable_description)
```

>
> Comparison 'Exact match vs. levenshtein at threshold 2 vs. anything else' of "postcode".
>
> Similarity is assessed using the following ComparisonLevels:
>
>    - 'Null' with SQL rule: "postcode_l" IS NULL OR "postcode_r" IS NULL
>    - 'Exact match' with SQL rule: "postcode_l" = "postcode_r"
>    - Levenshtein <= 2' with SQL rule: levenshtein("postcode_l", "postcode_r") <= 2
>    - 'All other comparisons' with SQL rule: ELSE

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

names = ['country_code', 'postal_code', 'place_name', 'admin_name1', 'admin_code1', 'admin_name2', 'admin_code2', 'admin_name3', 'admin_code3', 'latitude', 'longitude','accuracy']
postcodes = pd.read_csv("GB_full.txt", sep="\t", header = None, names=names)
postcodes.head(5)
```

|    | country_code   | postal_code   | place_name   | admin_name1   | admin_code1   | admin_name2   |   admin_code2 | admin_name3             | admin_code3   |   latitude |   longitude |   accuracy |
|---:|:---------------|:--------------|:-------------|:--------------|:--------------|:--------------|--------------:|:------------------------|:--------------|-----------:|------------:|-----------:|
|  0 | GB             | AL3 8QE       | Slip End     | England       | ENG           | Bedfordshire  |           nan | Central Bedfordshire    | E06000056     |    51.8479 |     -0.4474 |          6 |
|  1 | GB             | AL5 3NG       | Harpenden    | England       | ENG           | Bedfordshire  |           nan | Central Bedfordshire    | E06000056     |    51.8321 |     -0.383  |          6 |
|  2 | GB             | AL5 3NS       | Hyde         | England       | ENG           | Bedfordshire  |           nan | Central Bedfordshire    | E06000056     |    51.8333 |     -0.3763 |          6 |
|  3 | GB             | AL5 3QF       | Hyde         | England       | ENG           | Bedfordshire  |           nan | Central Bedfordshire    | E06000056     |    51.8342 |     -0.3851 |          6 |
|  4 | GB             | B10 0AB       | Birmingham   | England       | ENG           | West Midlands |           nan | Birmingham District (B) | E08000025     |    52.4706 |     -1.875  |          6 |

Now combine the lat-long coordinates from the `GB_full.txt` lookup.

```
df_with_coordinates = df.merge(postcodes[["postal_code", "latitude", "longitude"]], 
                                    left_on="postcode_fake", 
                                    right_on="postal_code",
                                    how="left")
df_with_coordinates = df_with_coordinates.rename({'postcode_fake':'postcode'}, axis=1)

df_with_coordinates.head()
```

|    | unique_id   | cluster   | full_name                                        | first_and_surname   | first_name   | surname   | dob        | birth_place   | postcode   | gender   | occupation   | postal_code   |   latitude |   longitude |
|---:|:------------|:----------|:-------------------------------------------------|:--------------------|:-------------|:----------|:-----------|:--------------|:-----------|:---------|:-------------|:--------------|-----------:|------------:|
|  0 | Q2296770-1  | Q2296770  | thomas clifford, 1st baron clifford of chudleigh | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF   | male     | politician   | TQ13 8DF      |    50.6927 |     -3.8139 |
|  1 | Q2296770-2  | Q2296770  | thomas of chudleigh                              | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF   | male     | politician   | TQ13 8DF      |    50.6927 |     -3.8139 |
|  2 | Q2296770-3  | Q2296770  | tom 1st baron clifford of chudleigh              | tom chudleigh       | tom          | chudleigh | 1630-08-01 | devon         | TQ13 8DF   | male     | politician   | TQ13 8DF      |    50.6927 |     -3.8139 |
|  3 | Q2296770-4  | Q2296770  | thomas 1st chudleigh                             | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8HU   |          | politician   | TQ13 8HU      |    50.6876 |     -3.8958 |
|  4 | Q2296770-5  | Q2296770  | thomas clifford, 1st baron chudleigh             | thomas chudleigh    | thomas       | chudleigh | 1630-08-01 | devon         | TQ13 8DF   |          | politician   | TQ13 8DF      |    50.6927 |     -3.8139 |

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

Phonetic transformation algorithms can be used to identify words that sound similar, even if they are spelled differently. These are particularly useful for names and can be used as an additional comparison level within name comparisons.

For a more detailed explanation on phonetic transformation algorithms, see the [topic guide](phonetic.md).

### Example

There are a number of python packages which support phonetic transformations that can be applied to a pandas dataframe, which can then be loaded into the DuckDBLinker. For example, creating a [Double Metaphone](phonetic.md#double-metaphone) column with the [phonetics](https://pypi.org/project/phonetics/) python library:

```
import pandas as pd
import phonetics

df = pd.read_parquet("PATH/TO/DATA/fake_1000.parquet")

# Define a function to apply the dmetaphone phonetic algorithm to each name in the column
def dmetaphone_name(name):
    if name is None:
        pass
    else:
        return phonetics.dmetaphone(name)

# Apply the function to the "first_name" and surname columns using the apply method
df['first_name_dm'] = df['first_name'].apply(dmetaphone_name)
df['surname_dm'] = df['surname'].apply(dmetaphone_name)

df.head()
```

|    |   unique_id | first_name   | surname   | dob        | city   | email                          |   group | first_name_dm   | surname_dm       |
|---:|------------:|:-------------|:----------|:-----------|:-------|:-------------------------------|--------:|:----------------|:-----------------|
|  0 |           0 | Julia        |           | 2015-10-29 | London | hannah88@powers.com            |       0 | ('JL', 'AL')    |                  |
|  1 |           1 | Julia        | Taylor    | 2015-07-31 | London | hannah88@powers.com            |       0 | ('JL', 'AL')    | ('TLR', '')      |
|  2 |           2 | Julia        | Taylor    | 2016-01-27 | London | hannah88@powers.com            |       0 | ('JL', 'AL')    | ('TLR', '')      |
|  3 |           3 | Julia        | Taylor    | 2015-10-29 |        | hannah88opowersc@m             |       0 | ('JL', 'AL')    | ('TLR', '')      |
|  4 |           4 | oNah         | Watson    | 2008-03-23 | Bolton | matthew78@ballard-mcdonald.net |       1 | ('AN', '')      | ('ATSN', 'FTSN') |

Note: [Soundex](phonetic.md#soundex) and [Metaphone](phonetic.md#metaphone) are also supported in [phoneitcs](https://pypi.org/project/phonetics/) 

Now that the dmetaphone columns have been added, they can be used within comparisons. For example, using the [name_comparison](../comparison_template_library.md#splink.comparison_template_library.NameComparisonBase) function from the [comparison template library](customising_comparisons.ipynb#name-comparisons).

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


