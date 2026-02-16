Consider splink/internals/blocking.py

The way blocking currently works is that each blocking rule filters out all previous pairs.

This means a deduplication step is not needed - we can simply UNION ALL the pairs created by each blocking rule without deduping.

However, this creates complications for exploding blocking rules.

Worse, it means that to evaluate a blocking rule, we need to project any column used by ANY blocking rule, which is inefficient for columnar engines like duckdb

I want instead Splink to move to a much simpler blocking strategy:
- Create the blocked pairs for each blocking rule separately
- UNION these pairs (i.e. create then dedupe, rather than dedupe during creation)

To give an example, we currently do blocking like like:


        select
            '0' as match_key,
            l."source_dataset" || '-__-' || l."unique_id" as join_key_l,
            r."source_dataset" || '-__-' || r."unique_id" as join_key_r
            from __splink__df_concat_with_tf as l
            inner join __splink__df_concat_with_tf as r
            on
            (l.first_name = r.first_name)
            where l."source_dataset" || '-__-' || l."unique_id" < r."source_dataset" || '-__-' || r."unique_id"

             UNION ALL
            select
            '1' as match_key,
            l."source_dataset" || '-__-' || l."unique_id" as join_key_l,
            r."source_dataset" || '-__-' || r."unique_id" as join_key_r
            from __splink__df_concat_with_tf as l
            inner join __splink__df_concat_with_tf as r
            on
            (l.surname = r.surname)
            where l."source_dataset" || '-__-' || l."unique_id" < r."source_dataset" || '-__-' || r."unique_id"
            AND NOT (coalesce((l.first_name = r.first_name),false))

and I'm thinking it should be more like:


      with blocking_rules as (
    select
        '0' as match_key,
        l."source_dataset" || '-__-' || l."unique_id" as join_key_l,
        r."source_dataset" || '-__-' || r."unique_id" as join_key_r
    from __splink__df_concat_with_tf as l
    inner join __splink__df_concat_with_tf as r
    on
    (l.first_name = r.first_name)
    where l."source_dataset" || '-__-' || l."unique_id" < r."source_dataset" || '-__-' || r."unique_id"

    UNION ALL

    select
        '1' as match_key,
        l."source_dataset" || '-__-' || l."unique_id" as join_key_l,
        r."source_dataset" || '-__-' || r."unique_id" as join_key_r
    from __splink__df_concat_with_tf as l
    inner join __splink__df_concat_with_tf as r
    on
    (l.surname = r.surname)
    where l."source_dataset" || '-__-' || l."unique_id" < r."source_dataset" || '-__-' || r."unique_id"
)
select
    min(match_key::int) as match_key,
    join_key_l,
    join_key_r
from blocking_rules
group by
    join_key_l,
    join_key_r


Implement this so it works across all the uses of blocking in the codebase (e.g. prediction, analyse blocking rules, and so on)