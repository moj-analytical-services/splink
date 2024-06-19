# Why do we need record linkage?

## In a perfect world


In a perfect world, everyone (and everything) would have a **single, unique identifier**. If this were the case, linking any datasets would be a simple inner join.

???+ example
    Consider 2 tables of people A and B with no duplicates and each person has a unique id `UID`. To join these tables in SQL we would write:

    ```
    SELECT *
    FROM A
    INNER JOIN B
    ON A.UID = B.UID
    ```


## In reality

Real datasets often lack truly unique identifiers (both within and across datasets).

The overall aim of record linkage is to **generate a unique identifier** to be used like `UID` to our "perfect world" scenario.

Record linkage the process of using the information within records to assess whether records refer to the same entity.  For example, if records refer to people, factors such as names, date of birth, location etc can be used to link records together.

Record linkage can be done within datasets (deduplication) or between datasets (linkage), or both.
