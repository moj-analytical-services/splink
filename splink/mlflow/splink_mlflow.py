import shutil
import mlflow
import os
import json

from ..charts import save_offline_chart

# ======================================================================
# ======================================================================
def _log_comparison_details(comparison):
    """
    Traverse a single comparison item from the splink settings JSON and log the sql condition the comparison was made
    under and the m and u values
    Parameters
    ----------
    comparison :

    Returns
    -------

    """
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
        if m_probability: # this means we don't log the null comparison as it doesnt have a m and u value
            mlflow.log_params(log_dict)


# ======================================================================
# ======================================================================
def _log_comparisons(splink_model_json):
    """
    Traverse the comparisons part of the splink settings and extract the learned values.
    This allows you to easily compare values across different training conditions.

    Parameters
    ----------
    splink_model_json : the settings json from a splink model.

    Returns
    -------

    """
    comparisons = splink_model_json['comparisons']
    for _ in comparisons:
        _log_comparison_details(_)


# ======================================================================
# ======================================================================
def _log_hyperparameters(splink_model_json):
    """
    Simple method for extracting parameters from the splink settings and logging as parameters in MLFlow
    Parameters
    ----------
    splink_model_json : the settings json from a splink model.

    Returns
    -------

    """
    hyper_param_keys = ['link_type', 'probability_two_random_records_match', 'sql_dialect', 'unique_id_column_name',
                        'em_convergence', 'retain_matching_columns', 'blocking_rules_to_generate_predictions']
    for key in hyper_param_keys:
        mlflow.log_param(key, splink_model_json[key])


# ======================================================================
# ======================================================================
def _log_splink_model_json(splink_model_json):
    """
    Log the splink settings json in its entirery under the name "linker.json"
    Parameters
    ----------
    splink_model_json : the settings json from a splink model.

    Returns
    -------

    """
    mlflow.log_dict(splink_model_json, "linker.json")


# ======================================================================
# ======================================================================
class splinkSparkMLFlowWrapper(mlflow.pyfunc.PythonModel):
    """
    Wrapper class necessary for storing and retrieving the splink model JSON from the MLFlow model repository.
    """

    def load_context(self, context):
        # this simply stores the json with the MLFLow model.
        self.linker_json = context.artifacts['linker_json_path']



# ======================================================================
# ======================================================================
def _save_splink_model_to_mlflow(linker, model_name):
    """
    Simple method for logging a splink model
    Parameters
    ----------
    linker : Splink model object
    model_name : name to save model under

    Returns
    -------

    """
    path = "linker.json"
    if os.path.isfile(path):
        os.remove(path)
    linker.save_settings_to_json(path)
    artifacts = {"linker_json_path": path}
    mlflow.pyfunc.log_model(model_name, python_model=splinkSparkMLFlowWrapper(), artifacts=artifacts)
    os.remove(path)


# ======================================================================
# ======================================================================
def log_splink_model_to_mlflow(linker, model_name, log_parameters_charts=True,
                               log_profiling_charts=False, params=None, metrics=None, artifacts=None):
    """
    Comprehensive logging of Splink attributes, parameters, charts and model JSON to MLFlow to provide easy
    reproducability and tracking during model development and deployment. This will create a new run under which
    to log everything. Additional parameters and metrics can be logged through the params and metrics arguments.

    For details on MLFlow please visit https://mlflow.org/.

    This will log
    - All the m and u values trained along with their training condition
    - All the hyperparameters set during training (blocking rules, link type, sql_dialect etc)

    Parameters
    ----------
    linker : a trained Splink linkage model
    model_name : str, the name to log the model under
    log_parameters_charts: boolean, whether to log parameter charts or not. Default to True as this is a quick operation.
    log_profiling_charts: boolean, whether to log data profiling charts or not. Default to False as this requires running
    Spark jobs and can take time.
    params : dict[str: str]. Dictionary is of param_name :param_value. Optional argument for logging arbitrary
    parameters
    metrics : dict[str: double or int]. Dictionary is of metric_name :metric_value. Optional argument for logging
    arbitrary metrics
    artifacts : dict[str: str]. Dictionary is of artifact_name :artifact_filepath. Optional argument for logging
    arbitrary artifacts (NB - artifacts must be written to disk before logging).
    Returns
    -------

    """

    splink_model_json = linker._settings_obj.as_dict()

    with mlflow.start_run() as run:
        _log_splink_model_json(splink_model_json)
        _log_hyperparameters(splink_model_json)
        _log_comparisons(splink_model_json)
        _save_splink_model_to_mlflow(linker, model_name)
        if log_profiling_charts or log_parameters_charts:
            _log_linker_charts(linker, log_parameters_charts, log_profiling_charts)
        if params:
            mlflow.log_params(params)
        if metrics:
            mlflow.log_metrics(metrics)
        if artifacts:
            mlflow.log_artifacts(artifacts)

    return run


# ======================================================================
# ======================================================================
def _log_linker_charts(linker, log_parameters_charts, log_profiling_charts):
    '''
    Log all the non-data related charts to MLFlow
    Parameters
    ----------
    linker : a Splink linker object
    log_parameters_charts: boolean, whether to log parameter charts or not
    log_profiling_charts: boolean, whether to log data profiling charts or not

    Returns
    -------

    '''

    charts_dict = {}
    if log_parameters_charts:
        weights_chart = linker.match_weights_chart()
        mu_chart = linker.m_u_parameters_chart()
        compare_chart = linker.parameter_estimate_comparisons_chart()
        charts_dict["weights_chart"] = weights_chart
        charts_dict["mu_chart"] = mu_chart
        charts_dict["compare_chart"] = compare_chart

    if log_profiling_charts:
        missingness_chart = linker.missingness_chart()
        # completeness = linker.completeness_chart() # Note to Splink team - this method behaves differently to the others
        unlinkables = linker.unlinkables_chart()
        blocking_rules_chart = linker.cumulative_num_comparisons_from_blocking_rules_chart()
        charts_dict["missingness_chart"] = missingness_chart
       # charts_dict["completeness"] = completeness
        charts_dict["unlinkables"] = unlinkables
        charts_dict["blocking_rules_chart"] = blocking_rules_chart

    for name, chart in charts_dict.items():
        _log_chart(name, chart)


# ======================================================================
# ======================================================================
def _log_chart(chart_name, chart):
    '''
    Save a chart to MLFlow. This writes the chart out temporarily for MLFlow to pick up and save.
    Parameters
    ----------
    chart_name : str, the name the chart will be given in MLFlow
    chart : chart object from Splink

    Returns
    -------

    '''
    path = f"{chart_name}.html"
    if os.path.isfile(path):
        os.remove(path)
    save_offline_chart(chart.spec, path)
    mlflow.log_artifact(path)
    os.remove(path)

# ======================================================================
# ======================================================================
def get_model_json(artifact_uri):
    """

    Returns a Splink model JSON from a model tracked with MLFlow. This can be retrieved either from a run or
    from the MLFlow Model Registry, if and only if the model was logged to MLFlow using the method
    splink.mlflow.log_splink_model_to_mlflow().

    Extracting the linker's JSON specification requires temporarily writing to /tmp on the local file system. All
    written files are deleted before the function returns.

    Parameters
    ----------
    artifact_uri : URI pointing to the artifacts, such as "runs:/500cf58bee2b40a4a82861cc31a617b1/my_model.pkl",
        "models:/my_model/Production", or "s3://my_bucket/my/file.txt".

    Returns
    -------
    A splink setting json as a python dict.

    """
    temp_file_path = f"/tmp/{artifact_uri.split('/')[1]}/splink_mlflow_artifacts_download"
    os.makedirs(temp_file_path)
    mlflow.artifacts.download_artifacts(
        artifact_uri=artifact_uri
        , dst_path=temp_file_path
    )

    with open(f"{temp_file_path}/artifacts/linker.json", "r") as f:
        linker_json = json.load(f)
    shutil.rmtree(temp_file_path)

    return linker_json
