import splink.duckdb.comparison_library as cl
from splink.duckdb.blocking_rule_library import block_on
from splink.duckdb.linker import DuckDBLinker
from splink.internals.datasets import splink_datasets

df = splink_datasets.historical_50k
df = df.head(1000)

settings_dict = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        block_on(["postcode_fake", "first_name"]),
        block_on(["first_name", "surname"]),
        block_on(["dob", "substr(postcode_fake,1,2)"]),
        block_on(["postcode_fake", "substr(dob,1,3)"]),
        block_on(["postcode_fake", "substr(dob,4,5)"]),
    ],
    "comparisons": [
        cl.exact_match(
            "first_name",
            term_frequency_adjustments=True,
        ),
        cl.jaro_winkler_at_thresholds(
            "surname",
            distance_threshold_or_thresholds=[0.9, 0.8],
        ),
        cl.levenshtein_at_thresholds(
            "postcode_fake", distance_threshold_or_thresholds=[1, 2]
        ),
    ],
}


linker = DuckDBLinker(df, settings_dict)

linker.estimate_u_using_random_sampling(max_pairs=1e6)

linker.estimate_parameters_using_expectation_maximisation(
    block_on(["first_name", "surname"])
)

linker.estimate_parameters_using_expectation_maximisation(
    block_on(["dob", "substr(postcode_fake, 1,3)"])
)

df_e = linker.predict()
