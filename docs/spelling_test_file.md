# General text

This is some text that does not contain a mistake

<!-- spelling mistake to check spellchecker hitting start of file -->
startoffilemistake

# colons

<!-- colon with no space after, words, then colon -->
With a massive thanks to external contributor [@hanslemm](https://github.com/moj-analytical-services/splink/pull/1191), Splink now supports :simple-postgresql: Postgres. To get started, check out the [Postgres Topic Guide](https://moj-analytical-services.github.io/splink/topic_guides/backends/postgres.html).

<!-- above rule with catch these too -->
[:simple-rss: RSS feed]
:octicons-duplicate-24:

<!-- colonmistake -->

## Code block ignore test

```
This shouldbeignored

```

<!-- codeblockmistake -->

## Card content ignore test

::cards::
[
    {
    "title": "`cumulative num comparisons from blocking rules chart`",
    "image": "./img/cumulative_num_comparisons_from_blocking_rules_chart.png",
    "url": "./cumulative_num_comparisons_from_blocking_rules_chart.ipynb"
    },
]
::/cards::

<!-- cardmistakes -->

## blog post headers
---
date: 2024-01-23
authors:
  - zoe-s
  - alice-o
categories:
  - Ethics
---

<!-- blogmistake -->

## markdown links

<!-- [my link](https://moj-analytical-services.github.io/splink/feed_rss_created.xml)! 

linkmistake -->

## python code

```py
import splink.duckdb.duckdb_comparison_level_library as cll
```

<!-- pythoncodeblock

codemistake -->

## inline code

`pdf`

inlinecodemista

## anchor tags

<a href="https://t.co/eXSNHHe2kc">https://t.co/eXSNHHe2kc</a>

<!-- anchortagss -->

## Finds `py`?

<!-- no -->
[Performing column lookups](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py)

<!-- no -->
??? note "Warnings in practice:"
    ```py
    import logging
    logger = logging.getLogger(__name__)

    logger.warning("My warning message")
    ```

    Which will print:

    > `My warning message`

    to both the console and your log file.

<!-- yes -->

[the exceptions.py script](https://github.com/moj-analytical-services/splink/blob/master/splink/exceptions.py).

pymistake

## Test text between `diff` blocks

??? info "Example of convergence output"

    ```diff
    @@ -1,9 +1,8 @@
    {
    -  
    ```

## links

[`create_function` function](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function)

<!-- ## Multiline code

```
from rapidfuzz.distance.Levenshtein import distance
conn = sqlite3.connect(":memory:")
conn.create_function("levenshtein", 2, distance)
``` -->
## text between `$$`

$$\textsf{DamerauLevenshtein}(s_1, s_2) = \min \lbrace \begin{array}{l}
\text{insertion , }
\text{deletion , } 
\text{substitution , }
\text{transposition}
\end{array} \rbrace $$


Almost 100%, say 98% $Longrightarrow m \approx 0.98$

## code blocks

    ```
    SELECT *
    FROM A
    LEFT JOIN B
    ON A.UID = B.UID
    ```


## partial words

Equi-join

## alt python blocks

```python
import jellyfish
jellyfish.jaro_similarity("MARTHA", "AMRTHA")


```

Jaro-Winkler similarity is a variation of [Jaro similarity](#jaro-similarity)


<!-- Spelling error to check reaching end of file -->
endoffilemistake


