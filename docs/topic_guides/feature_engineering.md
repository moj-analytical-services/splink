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

## Postcodes

Many datasets contain

## Phonetic transformations

Phonetic transformation algorithms can be used to identify words that sound similar, even if they are spelled differently. These are particularly useful for names. These algorithms can be used in the preprocessing step for data linking and can give an extra layer of fuzzy-matching to help with the blocking and comparison process.

For blocking rules, it allows for possible candidate pairs of entities with phonetically similar transforms to be considered for linking. This can result in a "fuzzier" [blocking process](/blocking_rules.md#the-purpose-of-blocking_rules_to_generate_predictions), which may be beneficial in certain instances.

Similarly, phonetically similar transforms offer another way to do fuzzy-matching within a [comparison](/customising_comparisons.ipynb).

Below are some examples of well known phonetic transformation algorithmns.

### Soundex

Soundex is a widely-used algorithm for phonetic matching. It was developed by Margaret K. Odell and Robert C. Russell in the early 1900s. The basic idea behind Soundex is to encode words based on their sounds, rather than their spelling. This allows words that sound similar to be encoded in the same way, even if they are spelled differently.

To encode a word with Soundex, the algorithm follows these steps:

```
Retain the first letter of the word.
Replace each of the following letters with the corresponding number:
B, F, P, V: 1
C, G, J, K, Q, S, X, Z: 2
D, T: 3
L: 4
M, N: 5
R: 6

Replace all other letters with the number 0.
```

As for an example of similar names having the same Soundex code, consider the names "Smith" and "Smythe". 
These names are spelled differently, but they have the same pronunciation, so they would be encoded as "S530" using Soundex.


### Double Metaphone

Double Metaphone is a more advanced algorithm for phonetic matching, developed by Lawrence Philips in the 1990s. It is based on the Soundex algorithm, but is designed to be more accurate and to handle a wider range of words.

To encode a word with Double Metaphone, the algorithm follows these steps:

```
Retain the first letter of the word.
Remove any vowels, except for the first letter.
Replace certain letters with other letters or combinations of letters, as follows:
B: B
C: X if followed by "ia" or "h" (e.g. "Cia" becomes "X", "Ch" becomes "X"), otherwise "S"
D: J if followed by "ge", "gy", "gi", otherwise "T"
G: J if followed by "g", "d", "i", "y", "e", otherwise "K"
K: K
L: L
M: M
N: N
P: F
Q: K
R: R
S: X if followed by "h" (e.g. "Sh" becomes "X"), otherwise "S"
T: X if followed by "ia" or "ch" (e.g. "Tia" becomes "X", "Tch" becomes "X"), otherwise "T"
V: F
X: KS
Z: S
```

For example, surnames such as Cone and Kohn are encoded in the same way as "KN" , because they sound similar even though they are spelled differently. 

### Creating Phonetic Transformation Features

There are a number of ways you can generate phonetic transformation columns within a dataset. Below are a few examples of how this can be achieved.

#### DuckDB

DuckDB does not currently support phonetic transformations, such as Dmetaphone. 

As a workaround, there are a number of python packages which support phonetic transformations that can be applied to a pandas dataframe, which can then be loaded into the DuckDBLinker. For example:
```
import pandas as pd
import phonetics
df = pd.read_parquet("/Users/rosskennedy/splink_demos/data/fake_1000.parquet")

# Define a function to apply the dmetaphone phonetic algorithm to each name in the column
def dmetaphone_name(name):
    if name is None:
        pass
    else:
        return phonetics.dmetaphone(name)[0]

# Apply the function to the "first_name" and surname columns using the apply method
df['first_name_dm'] = df['first_name'].apply(dmetaphone_name)
df['surname_dm'] = df['surname'].apply(dmetaphone_name)

df.head(10)

```

#### Spark


