# Splink User Guide


## Background

Record linkage is the process of using statistical and computational tools used to identify related records, remove duplicated entries,
and aggregate information. This process has also been have been referred to as `entity resolution`, `record linkage`, `data matching`,
`instance matching`, `data linkage`, `data cleaning`, `data fusion` and/or `data merging` .

The matching status of a candidate record pair is calculated through either deterministic linking method or probabilistic ones.
Candidate record pairs are compared using a set of comparison functions called comparators that allow for approximate (not exact) similarities.

However in order to make this process computationally tractable usually the process of blocking is used where in order to reduce the number of record pairs that need to be compared, only the most relevant pairs are processed.



## SQL backends


``` mermaid
graph LR
  A[Start] --> B{SQL Backend?};
  B -->|Spark| SP[spark backend];
  B --> |sqlite3| SL3[sqlite3 backend] ;
   B --> |duckdb| DDB[duckdb]] ;
```

