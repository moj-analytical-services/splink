from __future__ import annotations

import json
from pathlib import Path

import splink.comparison_library as cl

from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets
from splink.blocking_analysis import chart_comparisons_from_blocking_rules
from splink.exploratory import completeness_chart, profile_columns
from splink.internals.charts.themes import THEME_CATALOGUE
from splink.internals.splink_dataframe import SplinkDataFrame


def build_linker() -> tuple[Linker, SplinkDataFrame]:
	df = splink_datasets.historical_50k

	blocking_rules = [
		block_on("substr(first_name,1,3)", "substr(surname,1,4)"),
		block_on("surname", "dob"),
		block_on("first_name", "dob"),
		block_on("postcode_fake", "first_name"),
		block_on("postcode_fake", "surname"),
		block_on("dob", "birth_place"),
		block_on("substr(postcode_fake,1,3)", "dob"),
		block_on("substr(postcode_fake,1,3)", "first_name"),
		block_on("substr(postcode_fake,1,3)", "surname"),
		block_on("substr(first_name,1,2)", "substr(surname,1,2)", "substr(dob,1,4)"),
	]

	settings = SettingsCreator(
		link_type="dedupe_only",
		comparisons=[
			cl.ExactMatch("first_name").configure(term_frequency_adjustments=True),
			cl.JaroWinklerAtThresholds("surname").configure(
				term_frequency_adjustments=True
			),
			cl.DateOfBirthComparison("dob", input_is_string=True),
			cl.PostcodeComparison("postcode_fake"),
			cl.ExactMatch("birth_place").configure(term_frequency_adjustments=True),
			cl.ExactMatch("occupation").configure(term_frequency_adjustments=True),
		],
		blocking_rules_to_generate_predictions=blocking_rules,
		probability_two_random_records_match=0.01,
		additional_columns_to_retain=["cluster"],
		retain_intermediate_calculation_columns=True,
	)

	db_api = DuckDBAPI()
	sdf = db_api.register(df).query_sql(
		"SELECT *, "
		"CASE WHEN hash(unique_id) % 95 = 0 THEN unique_id ELSE NULL END AS missing_95, "
		"CASE WHEN hash(unique_id) % 3 = 0 THEN unique_id ELSE NULL END AS missing_3, "
		"CASE WHEN hash(unique_id) % 6 = 0 THEN unique_id ELSE NULL END AS missing_6 "
		"FROM {this}"
	)

	linker = Linker(sdf, settings)
	linker.training.estimate_u_using_random_sampling(max_pairs=1e2)
	linker.training.estimate_parameters_using_expectation_maximisation(block_on("surname"))

	return linker, sdf


def labels_table(sdf, df_pred_errors):
	return sdf.query_sql(
		f"""
		WITH all_labels AS (
			SELECT
				l.unique_id AS unique_id_l,
				r.unique_id AS unique_id_r,
				'left' AS source_dataset_l,
				'right' AS source_dataset_r,
				CASE WHEN l.cluster = r.cluster THEN 1 ELSE 0 END AS clerical_match_score
			FROM
				{{this}} AS l
			CROSS JOIN
				{{this}} AS r
		),
		good_labels AS (
			SELECT
				l.unique_id AS unique_id_l,
				r.unique_id AS unique_id_r,
				'left' AS source_dataset_l,
				'right' AS source_dataset_r,
				CASE WHEN l.cluster = r.cluster THEN 1 ELSE 0 END AS clerical_match_score
			FROM
				{{this}} AS l
			INNER JOIN
				{{this}} AS r
			ON l.cluster = r.cluster
		),
		maybes AS (
			SELECT
				unique_id_l, unique_id_r,
				'left' AS source_dataset_l, 'right' AS source_dataset_r,
				clerical_match_score
			FROM {df_pred_errors.physical_name}
			WHERE clerical_match_score = 0
		)
		SELECT * FROM all_labels USING SAMPLE 1000
		UNION ALL
		SELECT * FROM good_labels USING SAMPLE 1000
		UNION ALL
		SELECT * FROM maybes USING SAMPLE 1000
		"""
	)


def build_charts(linker: Linker, sdf) -> list[tuple[str, object]]:
	sess = linker.training.estimate_parameters_using_expectation_maximisation(
		block_on("birth_place", "dob")
	)

	df_pred = linker.inference.predict()

	df_pred_errors = linker.inference.predict(
		threshold_match_probability=0.7
	).query_sql(
		"SELECT *, CASE WHEN cluster_l = cluster_r THEN 1 ELSE 0 END AS clerical_match_score "
		"FROM {this} WHERE clerical_match_score = 0"
	)

	sdf_labels = labels_table(sdf, df_pred_errors)
	metrics = ["f1", "f2", "phi", "p4"]

	waterfall_records = linker.inference.predict(
		threshold_match_weight=2
	).as_record_list(limit=1)

	charts = [
		(
			"ProbabilityTwoRandomRecordsMatchIterationChart",
			sess.probability_two_random_records_match_iteration_chart(),
		),
		(
			"ParameterEstimateComparisonsChart",
			linker.visualisations.parameter_estimate_comparisons_chart(include_u=True),
		),
		("MUParametersChart", linker.visualisations.m_u_parameters_chart()),
		("MatchWeightsChart", linker.visualisations.match_weights_chart()),
		("TFAdjustmentChart", linker.visualisations.tf_adjustment_chart("first_name")),
		(
			"MatchWeightsHistogramChart",
			linker.visualisations.match_weights_histogram(df_pred),
		),
		("UnlinkablesChart", linker.evaluation.unlinkables_chart()),
		(
			"CumulativeBlockingRuleComparisonsGeneratedChart",
			chart_comparisons_from_blocking_rules(
				sdf,
				blocking_rules=[block_on("first_name"), block_on("surname")],
				link_type="dedupe_only",
			),
		),
		(
			"ThresholdSelectionToolChart",
			linker.evaluation.accuracy_analysis_from_labels_table(
				sdf_labels,
				output_type="threshold_selection",
				add_metrics=metrics,
			),
		),
		(
			"ROCChart",
			linker.evaluation.accuracy_analysis_from_labels_table(
				sdf_labels,
				output_type="roc",
				add_metrics=metrics,
			),
		),
		(
			"PrecisionRecallChart",
			linker.evaluation.accuracy_analysis_from_labels_table(
				sdf_labels,
				output_type="precision_recall",
				add_metrics=metrics,
			),
		),
		(
			"AccuracyChart",
			linker.evaluation.accuracy_analysis_from_labels_table(
				sdf_labels,
				output_type="accuracy",
				add_metrics=metrics,
			),
		),
		("CompletenessChart", completeness_chart(sdf)),
		(
			"WaterfallChart",
			linker.visualisations.waterfall_chart(waterfall_records, filter_nulls=False),
		),
		(
			"ProfileColumnsChart",
			profile_columns(sdf, ["first_name", "surname", "dob"]),
		),
	]

	return charts


def build_payload(charts: list[tuple[str, object]]) -> dict[str, object]:
	themes = list(THEME_CATALOGUE.keys())
	payload_charts = []

	for name, chart in charts:
		specs_by_theme = {}
		for theme in themes:
			chart.set_theme(theme)
			specs_by_theme[theme] = chart.chart_dict
		payload_charts.append({"title": name, "specsByTheme": specs_by_theme})

	return {
		"themes": themes,
		"defaultTheme": "alt",
		"charts": payload_charts,
	}


def write_themes_json() -> None:

	base_dir = Path(__file__).resolve().parent.parent
	payload_out_path = base_dir / "charts" / "img" / "theme_selector_payload.json"
	payload_out_path.parent.mkdir(parents=True, exist_ok=True)

	if payload_out_path.exists():
		return

	linker, sdf = build_linker()
	charts = build_charts(linker, sdf)
	payload = build_payload(charts)

	payload_out_path.write_text(
		json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
		encoding="utf-8",
	)

	print(f"Wrote {payload_out_path}")
