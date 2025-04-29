import os
from typing import NamedTuple

from pipelines import config, base_image
from kfp.dsl import component, Output, Dataset


@component(base_image=base_image)
def validate_activation_config_op(
    activation_type: str,
    activation_config: dict,
) -> NamedTuple("Outputs", [("valid", bool), ("config", dict),],):
    valid = activation_config is not None and activation_type in activation_config
    return valid, activation_config.get(activation_type, {})


@component(base_image=base_image)
def activation_ga4mp_op(
    project: str,
    dataset_id: str,
    run_id: str,
    cfg: dict,
    activation_log_table: Output[Dataset],
):
    import logging
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    from common.retry_policies import BIGQUERY_RETRY_POLICY
    from activation.ga4mp.main import run as run_activation_flow

    query, ga4_measurement_id, ga4_api_secret, ga4_mp_debug = (
        cfg["query"],
        cfg["ga4_measurement_id"],
        cfg["ga4_api_secret"],
        cfg["ga4_mp_debug"],
    )

    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig(labels={"vai-mlops": f"activation"})
    query_job = client.query(
        query=f"""
        CREATE OR REPLACE TABLE `{project}.{dataset_id}.tmp_activation_ga4mp_{run_id}`
        OPTIONS(
            expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)
        )
        AS
        {query}
    """,
        job_config=job_config,
        job_retry=BIGQUERY_RETRY_POLICY,
    )
    query_job.result()

    tbl = client.get_table(f"{project}.{dataset_id}.tmp_activation_ga4mp_{run_id}")
    logging.info(f"Activation query produced {tbl.num_rows}!")

    activation_log_table.metadata[
        "table_id"
    ] = f"`{project}.{dataset_id}.activation_ga4mp_log`"

    repo_name = dataset_id.replace("_", "-")
    run_activation_flow(
        {
            "project": project,
            "source_table": f"{project}.{dataset_id}.tmp_activation_ga4mp_{run_id}",
            "prediction_run_id": run_id,
            "temp_location": f"gs://{project}-{repo_name}-pipelines",
            "ga4_measurement_id": ga4_measurement_id,
            "ga4_api_secret": ga4_api_secret,
            "log_db_dataset": dataset_id,
            "use_api_validation": 1 if ga4_mp_debug else None,
            "direct": "direct",
            "direct_running_mode": "multi_processing",
            "direct_num_workers": 0,
        }
    )

    client.query(
        query=f"DROP TABLE `{project}.{dataset_id}.tmp_activation_ga4mp_{run_id}`",
        job_config=job_config,
        job_retry=BIGQUERY_RETRY_POLICY,
    ).result()

    # add labels to the log table
    try:
        tbl = client.get_table(f"{project}.{dataset_id}.activation_ga4mp_log")
        if "vai-mlops" not in tbl.labels:
            tbl.labels = {"vai-mlops": f"activation"}
            client.update_table(tbl, ["labels"])
    except NotFound:
        logging.info(
            "No activation log table was created. "
            "Likely due to activation query returning no rows."
        )


@component(base_image=base_image)
def activation_bq_routine_op(project: str, dataset_id: str, run_id: str, cfg: dict):
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    bqr = client.get_routine(f"{cfg['dataset_id']}.{cfg['routine_id']}")
    params = cfg.get("params", {})
    params.update(
        {  # you can use this params if you want, especially run_id since it's dynamic
            "gcp_project_id": project,
            "gcp_dataset_id": dataset_id,
            "run_id": run_id,
        }
    )

    args = []
    for arg in bqr.arguments:
        args.append(
            bigquery.ScalarQueryParameter(
                arg.name, arg.data_type.type_kind, params.get(arg.name, None)
            )
        )

    client.query(
        query=f"CALL `{str(bqr.reference)}`({', '.join([f'@{a.name}' for a in args])});",
        job_config=bigquery.QueryJobConfig(
            query_parameters=args, labels={"vai-mlops": f"activation"}
        ),
    ).result()
