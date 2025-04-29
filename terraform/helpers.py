import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import yaml
from jinja2 import Environment, BaseLoader
from kfp.registry import RegistryClient
from google.cloud import bigquery
from google.api_core import exceptions

from common.config import config_schema, SchemaError
from pipelines.pipeline_ops import compile_pipeline
from pipelines.pipeline_ops import schedule_pipeline, delete_schedules


class MissingConfig(Exception):
    pass


class ConfigError(Exception):
    pass


def validate_config(config_path=None):
    """
    Runs the "retrieve_config" function to check for errors. Used to validate the config file before running Terraform.

    Parameters
    ----------
    config_path : String, optional
        A path to the configuration YAML file, by default None
    """
    config = retrieve_config(config_path)
    if len(config) == 0:
        raise ConfigError("Configuration YAML is empty.")

    # special validation for BQML
    if config["model"]["type"] == "BQML" and config["model"].get(
        "create_model_params", None
    ):
        cmp = config["model"]["create_model_params"]
        for k, v in cmp.items():
            if k.upper() == "INPUT_LABEL_COLS":
                raise Exception(
                    f"{k} parameter in create_model_params is not allowed. Should be left as default: ['label']"
                )

            if k.upper() == "DATA_SPLIT_COL":
                raise Exception(
                    f"{k} parameter in create_model_params is not allowed. It is always set to: DATA_SPLIT_COL='data_split'"
                )

            if k.upper() == "DATA_SPLIT_METHOD":
                raise Exception(
                    f"{k} parameter in create_model_params is not allowed. It is always set to: DATA_SPLIT_METHOD='CUSTOM'"
                )

            if k.upper() == "HPARAM_TUNING_ALGORITHM":
                raise Exception(
                    f"{k} parameter in create_model_params is not allowed. It is always set to: HPARAM_TUNING_ALGORITHM='VIZIER_DEFAULT'"
                )

            if k.upper() == "MODEL_REGISTRY":
                raise Exception(
                    f"{k} parameter in create_model_params is not allowed. It is always set to: MODEL_REGISTRY='VERTEX_AI'"
                )

            if k.upper() == "VERTEX_AI_MODEL_ID":
                raise Exception(
                    f"{k} parameter in create_model_params is not allowed. It is set to your bq_dataset_id param in the config."
                )


# creates a config dictionary from the config.yaml
def retrieve_config(config_path=None):
    """
    Retrieves the contets of the configuration YAML that are necessary to run the system.

    Parameters
    ----------
    config_path : String, optional
        A path to the configuration YAML file, by default None

    Returns
    -------
    config : dict
        A dictionary with key-value pairs from the provided configuration YAML file

    Raises
    ------
    MissingConfig
        Raised if the system cannot locate a configuration YAML file by provided path, environment variable, or the default "config.yaml"
    ConfigError
        Raised if there is an error in the configuration YAML that raises a schema error during validation
    """
    if not config_path:
        try:
            config_path = os.environ["TF_VAR_config_file"]
        except KeyError:
            config_path = os.path.join(os.path.dirname(__file__), "..", "config.yaml")

    config = None
    try:
        with open(config_path, "r") as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                raise ConfigError("Error in provided configuration file:\n", exc)
    except FileNotFoundError as exc:
        raise MissingConfig("No configuration file found:\n", exc)
    try:
        config = config_schema.validate(config)
        return config
    except SchemaError as exc:
        raise ConfigError("Error in provided configuration file:\n", exc)


def validate_create_dataset_routine(config, do_sleep=None):
    if do_sleep is not None and isinstance(do_sleep, int) and do_sleep > 0:
        time.sleep(do_sleep)

    bq = bigquery.Client(project=config["gcp_project_id"])
    try:
        bq.get_routine(f"{config['bq_dataset_id']}.create_dataset")

    except exceptions.NotFound as nfe:
        nfe.message = (
            f"Validation check failed: create_dataset has not been successfully created! Please check your {config['bq_stored_procedure_path']} file. "
            + nfe.message
        )
        raise nfe

    try:
        t = bq.get_table(f"{config['bq_dataset_id']}.tmp_training")
        bq.delete_table(t)

    except exceptions.NotFound as nfe:
        nfe.message = (
            f"Validation check failed: create_dataset in MODE=TRAINING did not finish successfully! Please check your {config['bq_stored_procedure_path']} file. "
            + nfe.message
        )
        raise nfe

    try:
        t = bq.get_table(f"{config['bq_dataset_id']}.tmp_inference")
        bq.delete_table(t)

    except exceptions.NotFound as nfe:
        nfe.message = (
            f"Validation check failed: create_dataset in MODE=INFERENCE did not finish successfully! Please check your {config['bq_stored_procedure_path']} file."
            + nfe.message
        )
        raise nfe


def compile_training(config):
    """
    Compiles the training pipline

    Parameters
    ----------
    config : dict
        a dictionary of key-value pairs extracted from the config.yaml
    """
    if config["model"]["type"] == "BQML":
        from pipelines.training import training_pipeline_bqml as training_pipeline

        params = {
            "gcp_project_id": config["gcp_project_id"],
            "bq_dataset_id": config["bq_dataset_id"],
            "data_date_start_days_ago": config["training"]["data_date_start_days_ago"],
            "create_model_params": config["model"].get("create_model_params", {}),
            "keep_n_best_models": config["training"]["keep_n_best_models"],
        }

    elif config["model"]["type"] == "CUSTOM":
        from pipelines.training import training_pipeline_custom as training_pipeline

        params = {
            "gcp_project_id": config["gcp_project_id"],
            "gcp_region": config["gcp_region"],
            "bq_dataset_id": config["bq_dataset_id"],
            "data_date_start_days_ago": config["training"]["data_date_start_days_ago"],
            "custom_training_params": config["model"].get("custom_training_params", {}),
            "keep_n_best_models": config["training"]["keep_n_best_models"],
        }

    compile_pipeline(
        pipeline_name=pipeline_name_(config, "training"),
        pipeline_func=training_pipeline,
        template_path="training_pipeline.yaml",
        pipeline_parameters=params,
        enable_caching=False,
        type_check=True,
    )


def compile_prediction(config):
    """
    Compiles the prediction pipline

    Parameters
    ----------
    config : dict
        a dictionary of key-value pairs extracted from the config.yaml
    """
    # Activation query compile
    activation_config = config.get("activation", {})
    if "ga4mp" in activation_config:
        sqlx = None
        with open(activation_config["ga4mp"]["query_path"], "r") as stream:
            sqlx = stream.read()

        sqlx = Environment(loader=BaseLoader).from_string(sqlx)
        sql = sqlx.render(config)
        activation_config["ga4mp"]["query"] = sql

    # Compile and push
    if config["model"]["type"] == "BQML":
        from pipelines.prediction import prediction_pipeline_bqml as prediction_pipeline

        params = {
            "gcp_project_id": config["gcp_project_id"],
            "gcp_region": config["gcp_region"],
            "bq_dataset_id": config["bq_dataset_id"],
            "data_date_start_days_ago": config["prediction"][
                "data_date_start_days_ago"
            ],
            "activation_config": activation_config,
        }

    elif config["model"]["type"] == "CUSTOM":
        from pipelines.prediction import (
            prediction_pipeline_custom as prediction_pipeline,
        )

        params = {
            "gcp_project_id": config["gcp_project_id"],
            "gcp_region": config["gcp_region"],
            "bq_dataset_id": config["bq_dataset_id"],
            "data_date_start_days_ago": config["prediction"][
                "data_date_start_days_ago"
            ],
            "activation_config": activation_config,
        }

    compile_pipeline(
        pipeline_name=pipeline_name_(config, "prediction"),
        pipeline_func=prediction_pipeline,
        template_path="prediction_pipeline.yaml",
        pipeline_parameters=params,
        enable_caching=False,
        type_check=True,
    )


def deploy_pipeline(config, ptype):
    """
    Deploys a pipeline to Vertex AI

    Parameters
    ----------
    ptype: str
         TRAINING|PREDICTION
    config : dict
        a dictionary of key-value pairs extracted from the config.yaml
    """
    client = RegistryClient(
        host=f"https://{config['gcp_region']}-kfp.pkg.dev/{config['gcp_project_id']}/{config['bq_dataset_id'].replace('_', '-') + '-pipelines'}"
    )
    template_name, version_name = client.upload_pipeline(
        file_name=(
            "training_pipeline.yaml"
            if ptype == "TRAINING"
            else "prediction_pipeline.yaml"
            if ptype == "PREDICTION"
            else None
        ),
        tags=["v1", "latest"],
        extra_headers={
            "description": f"{'Training' if ptype == 'TRAINING' else 'Prediction' if ptype == 'PREDICTION' else 'N/A'} pipeline for {config['bq_dataset_id']}"
        },
    )
    print(template_name, version_name)


def get_pipeline_version(config, repo_name, pipeline_name):
    client = RegistryClient(
        host=f"https://{config['gcp_region']}-kfp.pkg.dev/{config['gcp_project_id']}/{repo_name}"
    )
    kfp_tpls = client.list_versions(package_name=pipeline_name)
    kfp_tpls = sorted(kfp_tpls, key=lambda x: x["createTime"], reverse=True)[0]
    return kfp_tpls["name"].split("/")[-1]


def pipeline_name_(config, ptype):
    repo_name = f"{config['bq_dataset_id'].replace('_', '-')}-pipelines"
    pipeline_name_postfix = config["model"]["type"].lower()

    return f"{repo_name}--{ptype}-{pipeline_name_postfix}"


def main(args):
    """
    A driver for the pipeline functionality in terraform

    Parameters
    ----------
    argv : list
        the commandline arguments passed to the system
        passed to main by default from the driver
    """
    config = retrieve_config(args.filename)
    repo_name = f"{config['bq_dataset_id'].replace('_', '-')}-pipelines"
    pipeline_template_uri_root = f"https://{config['gcp_region']}-kfp.pkg.dev/{config['gcp_project_id']}/{repo_name}"
    pipeline_root = f"gs://{config['gcp_project_id']}-{repo_name}"

    if args.training:
        if args.compile:
            compile_training(config)

        if args.deploy:
            deploy_pipeline(config, ptype="TRAINING")

        if args.schedule:
            v = get_pipeline_version(
                config, repo_name, pipeline_name_(config, "training")
            )

            schedule_pipeline(
                project_id=config["gcp_project_id"],
                region=config["gcp_region"],
                pipeline_name=pipeline_name_(config, "training"),
                pipeline_template_uri=f"{pipeline_template_uri_root}/{pipeline_name_(config, 'training')}/{v}",
                pipeline_root=pipeline_root,
                cron=config["training"]["cron"],
                max_concurrent_run_count=1,
                start_time=None,
                end_time=None,
                pipeline_sa=None,
            )

        if args.erase:
            delete_schedules(
                config["gcp_project_id"],
                config["gcp_region"],
                pipeline_name_(config, "training"),
            )

    elif args.prediction:
        if args.compile:
            compile_prediction(config)

        if args.deploy:
            deploy_pipeline(config, ptype="PREDICTION")

        if args.schedule:
            v = get_pipeline_version(
                config, repo_name, pipeline_name_(config, "prediction")
            )

            schedule_pipeline(
                project_id=config["gcp_project_id"],
                region=config["gcp_region"],
                pipeline_name=pipeline_name_(config, "prediction"),
                pipeline_template_uri=f"{pipeline_template_uri_root}/{pipeline_name_(config, 'prediction')}/{v}",
                pipeline_root=pipeline_root,
                cron=config["prediction"]["cron"],
                max_concurrent_run_count=1,
                start_time=None,
                end_time=None,
                pipeline_sa=None,
            )

        if args.erase:
            delete_schedules(
                config["gcp_project_id"],
                config["gcp_region"],
                pipeline_name_(config, "prediction"),
            )

    else:
        print("No mode specified. Terminating process")
        sys.exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Terraform Helper",
        description="Helps terraform execute certain commands that aren't supported by any terraform modules.",
        epilog='"That which we persist in doing becomes easier to do, not that the nature of the thing has changed but that our power to do has increased."\n\t-Ralph Waldo Emerson',
    )
    parser.add_argument("-f", "--filename", help="Path to config.yaml file.")
    parser.add_argument("-v", "--validate", action="store_true")
    parser.add_argument(
        "-vcdr", "--validate-create-dataset-routine", action="store_true"
    )

    parser.add_argument("-t", "--training", action="store_true")
    parser.add_argument("-p", "--prediction", action="store_true")
    parser.add_argument("-c", "--compile", action="store_true")
    parser.add_argument("-d", "--deploy", action="store_true")
    parser.add_argument("-s", "--schedule", action="store_true")
    parser.add_argument("-e", "--erase", action="store_true")
    args = parser.parse_args()

    if args.validate:
        validate_config(args.filename)

    elif args.validate_create_dataset_routine:
        config = retrieve_config(args.filename)
        validate_create_dataset_routine(config, do_sleep=60)

    else:
        main(args)
