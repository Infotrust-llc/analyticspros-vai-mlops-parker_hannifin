import os
from typing import List, Optional

from pipelines import config, base_image
from kfp.dsl import component, Output, Dataset, Model, Input, Metrics


@component(base_image=base_image)
def vai_get_default_model_op(
    project: str, region: str, dataset_id: str, default_model: Output[Model]
):
    import logging
    from google.cloud import aiplatform as aip
    from google.cloud.aiplatform_v1.types.model import ModelSourceInfo

    aip.init(project=project, location=region)
    models = aip.Model.list(filter=f'display_name="{dataset_id}"')

    model = None
    version = None
    for m in models:
        model = m
        model_registry = aip.ModelRegistry(model=m.name)
        for v in model_registry.list_versions():
            if "default" in v.version_aliases:
                version = v
                break

    model_type = model.gca_resource.model_source_info.source_type
    default_model.metadata = {
        "project_id": project,
        "dataset_id": dataset_id,
        "vertex_model_name": dataset_id,
    }

    if model_type == ModelSourceInfo.ModelSourceType.BQML:
        # the api doesn't yet expose bigQueryModelReference parameter it just tells you the model is BQML
        from google.cloud import bigquery

        client = bigquery.Client(project=project)
        bq_model = None
        for m in client.list_models(dataset_id):
            bqm = client.get_model(f"{project}.{dataset_id}.{m.model_id}")

            if (
                bqm.training_runs[0]["vertexAiModelId"] == version.model_resource_name
                and bqm.training_runs[0]["vertexAiModelVersion"] == version.version_id
            ):
                bq_model = bqm
                break

        default_model.metadata = {
            "model_name": bq_model.model_id,
            "model_id": f"{project}.{dataset_id}.{bq_model.model_id}",
        }

    elif model_type == ModelSourceInfo.ModelSourceType.CUSTOM:
        default_model.metadata.update(
            {
                "model_name": version.model_resource_name,
                "model_version": model.version_id,
                "model_id": f"{version.model_resource_name}@{version.version_id}",
            }
        )


@component(base_image=base_image)
def vai_list_models_op(project: str, region: str, dataset_id: str) -> List[dict]:
    from google.cloud import aiplatform as aip

    aip.init(project=project, location=region)
    models = aip.Model.list(filter=f'display_name="{dataset_id}"')

    models_list = []
    for m in models:
        model_registry = aip.ModelRegistry(model=m.name)
        for v in model_registry.list_versions():
            models_list.append(
                {
                    "project_id": project,
                    "dataset_id": dataset_id,
                    "vertex_model_name": dataset_id,
                    "model_name": v.model_resource_name,
                    "model_version": v.version_id,
                    "model_id": f"{v.model_resource_name}@{v.version_id}",
                }
            )

    return models_list


@component(base_image=base_image)
def vai_model_evaluate_op(
    project: str,
    region: str,
    run_id: str,
    model: dict,
    training_table: Input[Dataset],
    custom_training_params: dict,
    metrics: Output[Metrics],
):
    import json
    from io import BytesIO
    from google.cloud import aiplatform as aip
    from google.cloud import storage
    from common import CUSTOM_EVAL_DRIVER_PY_PATH

    vai_model = aip.Model(f"{model['model_id']}")

    dataset_id = model["dataset_id"]
    base_gcs_uri = f"gs://{project}-{dataset_id.replace('_', '-')}-pipelines"
    aip.init(project=project, location=region, staging_bucket=base_gcs_uri)

    # Run training container in EVAL mode
    custom_training_params.pop("model_serving_container_image_uri")
    container_uri = custom_training_params.pop("container_uri")
    args = custom_training_params.pop("args", {})
    args.update(
        {
            "mode": "EVAL",
            "bq-training-table-id": training_table.metadata["table_id"],
            "model-id": model["model_id"],
        }
    )

    job = aip.CustomJob.from_local_script(
        display_name=f"{dataset_id}-training_run_id_{run_id}_eval_v{vai_model.version_id}",
        script_path=CUSTOM_EVAL_DRIVER_PY_PATH,
        base_output_dir=f"{vai_model.uri.replace('/model', '')}",
        container_uri=container_uri,
        args=[f"--{k}={v}" for k, v in args.items()],
        labels={"vai-mlops": "training"},
        **custom_training_params,
    )

    job.run(sync=True)

    # pick up eval results dropped by evaluation
    gcs = storage.Client()
    eval_res = None
    with BytesIO() as stream:
        gcs.download_blob_to_file(
            blob_or_uri=f"{vai_model.uri}/metrics.json", file_obj=stream
        )
        stream.seek(0)
        eval_res = json.load(stream)

    # create Metrics artifact
    emn = eval_res.pop("eval_metric_name")
    for k, v in eval_res.items():
        metrics.log_metric(k, v)

    # push evaluation to BigQuery
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY
    from google.cloud.exceptions import NotFound

    client = bigquery.Client(project=project)
    model_eval_table_id = f"{dataset_id}.model_evals"

    q_statement = None
    try:
        client.get_table(model_eval_table_id)
        q_statement = f"""
            DELETE FROM `{model_eval_table_id}` 
            WHERE training_run_id = "{run_id}" AND model_name = "{model["model_id"]}";

            INSERT INTO `{model_eval_table_id}`
            """
    except NotFound:
        q_statement = f"""
            CREATE OR REPLACE TABLE `{model_eval_table_id}` 
            OPTIONS( labels = [("vai-mlops", "training")] )
            AS"""

    query = f"""
    {q_statement}
    SELECT
        '{run_id}' as training_run_id,
        '{model["model_id"]}' as model_name,
        '{emn}' as eval_metric_name,
        {eval_res[emn]} as eval_metric_value,
        [
            {", ".join([f"STRUCT('{k}' as name, {v} as value)" for k, v in eval_res.items()])}
        ] as metrics
    """

    job_config = bigquery.QueryJobConfig(labels={"vai-mlops": f"training"})
    client.query(
        query=query, job_config=job_config, job_retry=BIGQUERY_RETRY_POLICY
    ).result()


@component(base_image=base_image)
def vai_custom_training_op(
    project: str,
    region: str,
    run_id: str,
    dataset_id: str,
    training_table: Input[Dataset],
    custom_training_params: dict,
    model: Output[Model],
) -> None:
    from google.cloud import aiplatform as aip

    base_gcs_uri = f"gs://{project}-{dataset_id.replace('_', '-')}-pipelines"
    aip.init(project=project, location=region, staging_bucket=base_gcs_uri)

    models = aip.Model.list(
        filter=f'display_name="{dataset_id}"', order_by="update_time desc"
    )
    parent_model = models[0] if len(models) > 0 else None

    container_uri = custom_training_params.pop("container_uri")
    model_serving_container_image_uri = custom_training_params.pop(
        "model_serving_container_image_uri"
    )
    job = aip.CustomContainerTrainingJob(
        display_name=f"{dataset_id}-training_run_id_{run_id}",
        container_uri=container_uri,
        model_serving_container_image_uri=model_serving_container_image_uri,
        model_serving_container_predict_route="/predict",
        model_serving_container_health_route="/health",
        labels={"vai-mlops": f"training"},
    )

    args = custom_training_params.pop("args", {})
    args.update(
        {
            "mode": "TRAINING",
            "bq-training-table-id": training_table.metadata["table_id"],
        }
    )
    model_registered = job.run(
        parent_model=parent_model.resource_name if parent_model else None,
        model_display_name=dataset_id,
        base_output_dir=f"{base_gcs_uri}/training_run_id_{run_id}",
        model_labels={"vai-mlops-training-run-id": run_id},
        args=[f"--{k}={v}" for k, v in args.items()],
        is_default_version=False,
        sync=True,
        **custom_training_params,
    )

    model_registered.wait()

    model.metadata = {
        "project_id": project,
        "dataset_id": dataset_id,
        "vertex_model_name": dataset_id,
        "model_name": model_registered.resource_name,
        "model_version": model_registered.version_id,
        "model_id": f"{model_registered.resource_name}@{model_registered.version_id}",
    }


@component(base_image=base_image)
def vai_batch_prediction_op(
    project: str,
    dataset_id: str,
    run_id: str,
    inference_table: Input[Dataset],
    predictions_table: Output[Dataset],
    model: Input[Model],
    job_name_prefix: str = "vai-mlops",
    machine_type: str = "n1-standard-4",
    max_replica_count: int = 10,
    batch_size: int = 1024,
    accelerator_count: int = 0,
    accelerator_type: str = None,
    generate_explanation: bool = False,
    dst_table_expiration_hours: int = 4,
):
    import logging
    from datetime import datetime, timedelta, timezone
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY
    from google.cloud.exceptions import NotFound
    from google.cloud.aiplatform import Model

    client = bigquery.Client(project=project)

    # run batch predictions
    vai_model = Model(f"{model.metadata['model_id']}")
    predictions_table_id = f"{project}.{dataset_id}.predictions"
    tmp_dst_prediction_table_id = f"{project}.{dataset_id}.temp_predictions_{run_id}"

    bq_table = client.get_table(inference_table.metadata["table_id"])
    columns = [c.name for c in bq_table.schema]

    batch_prediction_job = vai_model.batch_predict(
        job_display_name=f"{job_name_prefix}-prediction-run-id-{run_id}",
        instances_format="bigquery",
        predictions_format="bigquery",
        bigquery_source=f"bq://{inference_table.metadata['table_id']}",
        bigquery_destination_prefix=f"bq://{tmp_dst_prediction_table_id}",
        model_parameters={"columns": columns},
        machine_type=machine_type,
        max_replica_count=max_replica_count,
        batch_size=batch_size,
        accelerator_count=accelerator_count,
        accelerator_type=accelerator_type,
        generate_explanation=generate_explanation,
        labels={"vai-mlops": f"inference"},
    )

    batch_prediction_job.wait()

    # set temp table expiration
    if dst_table_expiration_hours > 0:
        table = client.get_table(tmp_dst_prediction_table_id)
        expiration = datetime.now(timezone.utc) + timedelta(
            hours=dst_table_expiration_hours
        )
        table.expires = expiration
        client.update_table(table, ["expires"])

    logging.info(batch_prediction_job.to_dict())

    # merge temp predictions table into main predictions table
    q_statement = None
    try:
        client.get_table(predictions_table_id)
        q_statement = f"""
            DELETE FROM `{predictions_table_id}` 
            WHERE prediction_run_id = "{run_id}" AND model_name = "{model.metadata["model_id"]}";

            INSERT INTO `{predictions_table_id}`
            """
    except NotFound:
        q_statement = f"""
            CREATE OR REPLACE TABLE `{predictions_table_id}` 
            PARTITION BY date
            CLUSTER BY prediction_run_id
            AS"""

    query = f"""
            {q_statement}
            SELECT 
                "{run_id}" as prediction_run_id,
                "{model.metadata["model_id"]}" as model_name,
                * 
            FROM `{tmp_dst_prediction_table_id}`;
        """

    query_job = client.query(query=query, job_retry=BIGQUERY_RETRY_POLICY)
    query_job.result()

    predictions_table.metadata["table_id"] = predictions_table_id


@component(base_image=base_image)
def vai_model_cleanup_op(
    project: str,
    dataset_id: str,
    keep_n_best_models: Optional[int],
    eval_metrics: Optional[List[Metrics]],
    best_model: Output[Model],
    metrics: Output[Metrics],
):
    import logging
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY
    from google.cloud import aiplatform

    client = bigquery.Client(project=project)

    eval_metric = "eval_metric_value"
    query = f"""
            SELECT
              model_name, {eval_metric} as eval_metric
            FROM `{project}.{dataset_id}.model_evals`
            WHERE TRUE
            QUALIFY 
              MAX(training_run_id) OVER () = training_run_id
            ORDER BY {eval_metric} DESC
        """

    job_config = bigquery.QueryJobConfig(labels={"vai-mlops": f"training"})
    query_job = client.query(
        query=query, job_config=job_config, job_retry=BIGQUERY_RETRY_POLICY
    )
    r = query_job.result()
    r = list(r)

    model_id = r[0]["model_name"]
    model_name, model_version = model_id.split("@")
    best_model.metadata = {
        "project_id": project,
        "dataset_id": dataset_id,
        "vertex_model_name": dataset_id,
        "model_name": model_name,
        "model_version": model_version,
        "model_id": model_id,
    }

    metrics.log_metric(eval_metric, r[0]["eval_metric"])

    # set best model as default in VAI Model Registry
    vai_model = aiplatform.ModelRegistry(model=model_name)
    vai_model.add_version_aliases(["default"], model_version)

    if keep_n_best_models is None or keep_n_best_models <= 1:
        return

    # delete models outside of top keep_n_best
    if (
        len(r) <= keep_n_best_models
    ):  # don't delete anything, we don't have too many models
        return

    for m in r[keep_n_best_models:]:
        model_name, model_version = m["model_name"].split("@")
        logging.info(f"Deleting model version: `{model_version}`")
        client.delete_model(model_id)
        vai_model.delete_version(model_version)
