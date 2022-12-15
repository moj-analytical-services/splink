import shutil
import mlflow
import os
import json

from ..charts import save_offline_chart

# ======================================================================
# ======================================================================
def _log_comparison_details(comparison):
    output_column_name = comparison['output_column_name']

    comparison_levels = comparison['comparison_levels']
    for _ in comparison_levels:
        sql_condition = _.get('sql_condition')
        m_probability = _.get('m_probability')
        u_probability = _.get('u_probability')
        log_dict = {
            f"output column {output_column_name} compared through condition {sql_condition} m_probability": m_probability,
            f"output column {output_column_name} compared through condition {sql_condition} u_probability": u_probability

        }
        if m_probability:
            mlflow.log_params(log_dict)


# ======================================================================
# ======================================================================
def _log_comparisons(splink_model_json):
    comparisons = splink_model_json['comparisons']
    for _ in comparisons:
        _log_comparison_details(_)


# ======================================================================
# ======================================================================
def _log_hyperparameters(splink_model_json):
    hyper_param_keys = ['link_type', 'probability_two_random_records_match', 'sql_dialect', 'unique_id_column_name',
                        'em_convergence', 'retain_matching_columns', 'blocking_rules_to_generate_predictions']
    for key in hyper_param_keys:
        mlflow.log_param(key, splink_model_json[key])


# ======================================================================
# ======================================================================
def _log_splink_model_json(splink_model_json):
    mlflow.log_dict(splink_model_json, "linker.json")


# ======================================================================
# ======================================================================
class splinkSparkMLFlowWrapper(mlflow.pyfunc.PythonModel):

    def load_context(self, context):
        self.linker_json = context.artifacts['linker_json_path']

    def model_json(self):
        return self.json


# ======================================================================
# ======================================================================
def _save_splink_model_to_mlflow(linker, model_name):
    path = "linker.json"
    if os.path.isfile(path):
        os.remove(path)
    linker.save_settings_to_json(path)
    artifacts = {"linker_json_path": path}
    mlflow.pyfunc.log_model(model_name, python_model=splinkSparkMLFlowWrapper(), artifacts=artifacts)
    os.remove(path)


# ======================================================================
# ======================================================================
def log_splink_model_to_mlflow(linker, model_name="linker", log_charts=True):
    splink_model_json = linker._settings_obj.as_dict()

    with mlflow.start_run() as run:
        _log_splink_model_json(splink_model_json)
        _log_hyperparameters(splink_model_json)
        _log_comparisons(splink_model_json)
        _save_splink_model_to_mlflow(linker, model_name)
        if log_charts:
            _log_linker_charts(linker)

    return run


# ======================================================================
# ======================================================================
def _log_linker_charts(linker):
    weights_chart = linker.match_weights_chart()
    mu_chart = linker.m_u_parameters_chart()
    missingness_chart = linker.missingness_chart()
    # completeness = linker.completeness_chart()
    compare_chart = linker.parameter_estimate_comparisons_chart()
    unlinkables = linker.unlinkables_chart()
    blocking_rules_chart = linker.cumulative_num_comparisons_from_blocking_rules_chart()
    charts_dict = {
        "weights_chart": weights_chart,
        "mu_chart": mu_chart,
        "missingness_chart": missingness_chart,
        #  "completeness": completeness,
        "compare_chart": compare_chart,
        "unlinkables": unlinkables,
        "blocking_rules_chart": blocking_rules_chart,
    }
    for name, chart in charts_dict.items():
        _log_chart(name, chart)


# ======================================================================
# ======================================================================
def _log_chart(chart_name, chart):
    path = f"{chart_name}.html"
    if os.path.isfile(path):
        os.remove(path)
    save_offline_chart(chart.spec, path)
    mlflow.log_artifact(path)


# ======================================================================
# ======================================================================
def get_model_json(artifact_uri):
    """
    Returns a Splink model JSON from a model tracked with MLFlow. This can be retrieved either from a run or
    from the MLFlow Model Registry, if and only if the model was logged to MLFlow using the method
    splink.mlflow.log_splink_model_to_mlflow().

    Extracting the linker's JSON specification requires temporarily writing to /tmp on the local file system. All
    written files are deleted before the function returns.

    Params:
        - artifact_uri – URI pointing to the artifacts, such as "runs:/500cf58bee2b40a4a82861cc31a617b1/my_model.pkl",
        "models:/my_model/Production", or "s3://my_bucket/my/file.txt".
    """
    temp_file_path = f"/tmp/{model_uri.split('/')[1]}/splink_mlflow_artifacts_download"
    os.makedirs(temp_file_path)
    mlflow.artifacts.download_artifacts(
        artifact_uri=artifact_uri
        , dst_path=temp_file_path
    )

    with open(f"{temp_file_path}/artifacts/linker.json", "r") as f:
        linker_json = json.load(f)
    shutil.rmtree(temp_file_path)

    return linker_json
