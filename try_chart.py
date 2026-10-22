import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

df = splink_datasets.historical_50k
db_api = DuckDBAPI()
df_sdf = db_api.register(df)

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("postcode_fake"),
        cl.ExactMatch("birth_place").configure(term_frequency_adjustments=True),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name", "surname"),
        block_on("dob"),
        block_on("postcode_fake"),
        block_on("first_name", "birth_place"),
        block_on("first_name", "surname", "dob"),
    ],
)

linker = Linker(df_sdf, settings)

linker.training.estimate_probability_two_random_records_match(
    [block_on("first_name", "surname", "dob")],
    recall=0.7,
)
linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)
linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("first_name", "surname"),
    max_pairs=100_000,
)
linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("dob"),
    max_pairs=100_000,
)

df_predict = linker.inference.predict(warning_mode="never")
cumulative_chart = linker.blocking_analysis.chart_comparisons_from_blocking_rules(
    record_sample_proportion=1.0
)
cumulative_chart.save("blocking_rule_performance_chart.html")

marginal_chart = linker.blocking_analysis.chart_blocking_rule_importance(df_predict)
marginal_chart.save("blocking_rule_marginal_contributions_chart.html")
marginal_chart
importance = marginal_chart.raw_records
