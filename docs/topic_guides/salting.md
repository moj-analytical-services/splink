---
tags:
  - Performance
  - Salting
  - Spark
---

# Salting blocking rules

For very large linkages using Apache Spark, Splink supports salting blocking rules.

Under certain conditions, this can help Spark better parallelise workflows, leading to shorter run times, and avoiding out of memory errors. It is most likely to help where you have blocking rules that create very large numbers of comparisons (100m records+) and where there is skew in how record comparisons are made (e.g. blocking on full name creates more comparisons amongst 'John Smith's than many other names).

Further information about the motivation for salting can be found [here](https://github.com/moj-analytical-services/splink/issues/527).

**Note that salting is only available for the Spark backend**

## How to use salting

To enable salting using the `SparkLinker`, you provide some of your blocking rules as a dictionary rather than a string.

This enables you to choose the number of salts for each blocking rule.

Blocking rules provided as plain strings default to no salting (`salting_partitions = 1`)

The following code snippet illustrates:

```
import logging

from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession

from splink.spark.linker import SparkLinker
from splink.spark.comparison_library import levenshtein_at_thresholds, exact_match

conf = SparkConf()
conf.set("spark.driver.memory", "12g")
conf.set("spark.sql.shuffle.partitions", "8")
conf.set("spark.default.parallelism", "8")

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)


settings = {
    "probability_two_random_records_match": 0.01,
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.dob = r.dob",
        {"blocking_rule": "l.first_name = r.first_name", "salting_partitions": 4},
    ],
    "comparisons": [
        levenshtein_at_thresholds("first_name", 2),
        exact_match("surname"),
        exact_match("dob"),
        exact_match("city", term_frequency_adjustments=True),
        exact_match("email"),
    ],
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
    "additional_columns_to_retain": ["group"],
    "max_iterations": 1,
    "em_convergence": 0.01,
}


df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)


linker = SparkLinker(df, settings)
logging.getLogger("splink").setLevel(5)

linker.load_settings(settings)
linker.deterministic_link()

```

And we can see that salting has been applied by looking at the SQL generated in the log:

```
SELECT
  l.unique_id AS unique_id_l,
  r.unique_id AS unique_id_r,
  l.first_name AS first_name_l,
  r.first_name AS first_name_r,
  l.surname AS surname_l,
  r.surname AS surname_r,
  l.dob AS dob_l,
  r.dob AS dob_r,
  l.city AS city_l,
  r.city AS city_r,
  l.tf_city AS tf_city_l,
  r.tf_city AS tf_city_r,
  l.email AS email_l,
  r.email AS email_r,
  l.`group` AS `group_l`,
  r.`group` AS `group_r`,
  '0' AS match_key
FROM __splink__df_concat_with_tf AS l
INNER JOIN __splink__df_concat_with_tf AS r
  ON l.dob = r.dob
WHERE
  l.unique_id < r.unique_id
UNION ALL
SELECT
  l.unique_id AS unique_id_l,
  r.unique_id AS unique_id_r,
  l.first_name AS first_name_l,
  r.first_name AS first_name_r,
  l.surname AS surname_l,
  r.surname AS surname_r,
  l.dob AS dob_l,
  r.dob AS dob_r,
  l.city AS city_l,
  r.city AS city_r,
  l.tf_city AS tf_city_l,
  r.tf_city AS tf_city_r,
  l.email AS email_l,
  r.email AS email_r,
  l.`group` AS `group_l`,
  r.`group` AS `group_r`,
  '1' AS match_key
FROM __splink__df_concat_with_tf AS l
INNER JOIN __splink__df_concat_with_tf AS r
  ON l.first_name = r.first_name
  AND CEIL(l.__splink_salt * 4) = 1
  AND NOT (
    COALESCE((
        l.dob = r.dob
    ), FALSE)
  )
WHERE
  l.unique_id < r.unique_id
UNION ALL
SELECT
  l.unique_id AS unique_id_l,
  r.unique_id AS unique_id_r,
  l.first_name AS first_name_l,
  r.first_name AS first_name_r,
  l.surname AS surname_l,
  r.surname AS surname_r,
  l.dob AS dob_l,
  r.dob AS dob_r,
  l.city AS city_l,
  r.city AS city_r,
  l.tf_city AS tf_city_l,
  r.tf_city AS tf_city_r,
  l.email AS email_l,
  r.email AS email_r,
  l.`group` AS `group_l`,
  r.`group` AS `group_r`,
  '1' AS match_key
FROM __splink__df_concat_with_tf AS l
INNER JOIN __splink__df_concat_with_tf AS r
  ON l.first_name = r.first_name
  AND CEIL(l.__splink_salt * 4) = 2
  AND NOT (
    COALESCE((
        l.dob = r.dob
    ), FALSE)
  )
WHERE
  l.unique_id < r.unique_id
UNION ALL
SELECT
  l.unique_id AS unique_id_l,
  r.unique_id AS unique_id_r,
  l.first_name AS first_name_l,
  r.first_name AS first_name_r,
  l.surname AS surname_l,
  r.surname AS surname_r,
  l.dob AS dob_l,
  r.dob AS dob_r,
  l.city AS city_l,
  r.city AS city_r,
  l.tf_city AS tf_city_l,
  r.tf_city AS tf_city_r,
  l.email AS email_l,
  r.email AS email_r,
  l.`group` AS `group_l`,
  r.`group` AS `group_r`,
  '1' AS match_key
FROM __splink__df_concat_with_tf AS l
INNER JOIN __splink__df_concat_with_tf AS r
  ON l.first_name = r.first_name
  AND CEIL(l.__splink_salt * 4) = 3
  AND NOT (
    COALESCE((
        l.dob = r.dob
    ), FALSE)
  )
WHERE
  l.unique_id < r.unique_id
UNION ALL
SELECT
  l.unique_id AS unique_id_l,
  r.unique_id AS unique_id_r,
  l.first_name AS first_name_l,
  r.first_name AS first_name_r,
  l.surname AS surname_l,
  r.surname AS surname_r,
  l.dob AS dob_l,
  r.dob AS dob_r,
  l.city AS city_l,
  r.city AS city_r,
  l.tf_city AS tf_city_l,
  r.tf_city AS tf_city_r,
  l.email AS email_l,
  r.email AS email_r,
  l.`group` AS `group_l`,
  r.`group` AS `group_r`,
  '1' AS match_key
FROM __splink__df_concat_with_tf AS l
INNER JOIN __splink__df_concat_with_tf AS r
  ON l.first_name = r.first_name
  AND CEIL(l.__splink_salt * 4) = 4
  AND NOT (
    COALESCE((
        l.dob = r.dob
    ), FALSE)
  )
WHERE
  l.unique_id < r.unique_id
```
