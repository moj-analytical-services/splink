import inspect

from splink import DuckDBAPI, Linker, splink_datasets
from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.settings_creator import SettingsCreator

# Mock objects for instantiation, replace with real ones if available
# For instance, you might need to pass actual dataframes or database tables
mock_settings = SettingsCreator(
    link_type="dedupe_only"
)  # or pass a real settings object
mock_db_api = DuckDBAPI()  # or pass a real database API subclass instance

# Instantiate the Linker object
linker = Linker(
    input_table_or_tables=splink_datasets.fake_1000,  # replace with real input
    settings=mock_settings,
    db_api=mock_db_api,
)

# List of specific methods you want to extract docstrings from
methods = [
    # Linker.inference methods
    "inference.deterministic_link",
    "inference.predict",
    "inference.find_matches_to_new_records",
    "inference.compare_two_records",
    # Linker.training methods
    "training.estimate_probability_two_random_records_match",
    "training.estimate_u_using_random_sampling",
    "training.estimate_parameters_using_expectation_maximisation",
    "training.estimate_m_from_pairwise_labels",
    "training.estimate_m_from_label_column",
    # Linker.visualisations methods
    "visualisations.match_weights_chart",
    "visualisations.m_u_parameters_chart",
    "visualisations.match_weights_histogram",
    "visualisations.parameter_estimate_comparisons_chart",
    "visualisations.tf_adjustment_chart",
    "visualisations.waterfall_chart",
    "visualisations.comparison_viewer_dashboard",
    "visualisations.cluster_studio_dashboard",
    # Linker.clustering methods
    "clustering.cluster_pairwise_predictions_at_threshold",
    "clustering.compute_graph_metrics",
    # Linker.evaluation methods
    "evaluation.prediction_errors_from_labels_table",
    "evaluation.accuracy_analysis_from_labels_column",
    "evaluation.accuracy_analysis_from_labels_table",
    "evaluation.prediction_errors_from_labels_column",
    "evaluation.unlinkables_chart",
    "evaluation.labelling_tool_for_specific_record",
    # Linker.table_management methods
    "table_management.compute_tf_table",
    "table_management.invalidate_cache",
    "table_management.register_table_input_nodes_concat_with_tf",
    "table_management.register_table_predict",
    "table_management.register_term_frequency_lookup",
    "table_management.register_table",
    # Linker.misc methods
    "misc.save_model_to_json",
    "misc.query_sql",
]


# Function to extract docstrings from methods of the Linker class instance
def extract_method_docstrings(linker_instance, method_list):
    docstrings = {}
    for method_path in method_list:
        components = method_path.split(".")
        method_obj = linker_instance
        for component in components:
            method_obj = getattr(method_obj, component)
        docstring = inspect.getdoc(method_obj)
        docstrings[f"{linker_instance.__class__.__name__}.{method_path}"] = (
            docstring if docstring else "No docstring available"
        )
    return docstrings


# Saving the docstrings to a text file
def save_docstrings(docstrings, filename="docstrings.txt"):
    with open(filename, "w", encoding="utf-8") as file:
        for method_path, docstring in docstrings.items():
            file.write(f"{method_path}:\n")
            file.write(f"{docstring}\n\n")


# Main execution
if __name__ == "__main__":
    docstrings = extract_method_docstrings(linker, methods)
    save_docstrings(docstrings)
    print("Docstrings extracted and saved to docstrings.txt")
