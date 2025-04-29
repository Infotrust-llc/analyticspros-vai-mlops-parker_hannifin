import logging
from typing import Any, Callable, Dict, Optional

import google.auth
import google.auth.credentials as credentials
import requests
import yaml
from google.cloud.aiplatform.pipeline_jobs import PipelineJob, _set_enable_caching_value
from google.cloud.aiplatform import PipelineJobSchedule
from kfp import compiler
from kfp.registry import RegistryClient


def compile_pipeline(
    pipeline_func: Callable,
    template_path: str,
    pipeline_name: str,
    pipeline_parameters: Optional[Dict[str, Any]] = None,
    enable_caching: bool = False,
    type_check: bool = True,
) -> str:
    compiler.Compiler().compile(
        pipeline_func=pipeline_func,
        package_path=template_path,
        pipeline_name=pipeline_name,
        pipeline_parameters=pipeline_parameters,
        type_check=type_check,
    )

    with open(template_path, "r") as file:
        configuration = yaml.safe_load(file)

    _set_enable_caching_value(
        pipeline_spec=configuration, enable_caching=enable_caching
    )

    with open(template_path, "w") as yaml_file:
        yaml.dump(configuration, yaml_file)

    return template_path


def run_pipeline_from_func(
    pipeline_func: Callable,
    pipeline_root: str,
    project_id: str,
    location: str,
    service_account: str,
    pipeline_parameters: Optional[Dict[str, Any]],
    enable_caching: bool = False,
    experiment_name: str = None,
    job_id: str = None,
    labels: Optional[Dict[str, str]] = None,
    credentials: Optional[credentials.Credentials] = None,
    encryption_spec_key_name: Optional[str] = None,
    wait: bool = False,
) -> str:
    pl = PipelineJob.from_pipeline_func(
        pipeline_func=pipeline_func,
        parameter_values=pipeline_parameters,
        enable_caching=enable_caching,
        job_id=job_id,
        output_artifacts_gcs_dir=pipeline_root,
        project=project_id,
        location=location,
        credentials=credentials,
        encryption_spec_key_name=encryption_spec_key_name,
        labels=labels,
    )
    pl.submit(service_account=service_account, experiment_name=experiment_name)

    if wait:
        pl.wait()
        if pl.has_failed:
            raise RuntimeError("Pipeline execution failed")
    return pl


def upload_pipeline_artefact_registry(
    template_path: str,
    project_id: str,
    region: str,
    repo_name: str,
    tags: list = None,
    description: str = None,
) -> str:
    host = f"https://{region}-kfp.pkg.dev/{project_id}/{repo_name}"
    client = RegistryClient(host=host)
    response = client.upload_pipeline(
        file_name=template_path, tags=tags, extra_headers={"description": description}
    )
    logging.info(f"Pipeline uploaded : {host}")
    logging.info(response)
    return response[0]


def delete_pipeline_artefact_registry(
    project_id: str, region: str, repo_name: str, package_name: str
) -> str:
    host = f"https://{region}-kfp.pkg.dev/{project_id}/{repo_name}"
    client = RegistryClient(host=host)
    response = client.delete_package(package_name=package_name)
    logging.info(f"Pipeline deleted : {package_name}")
    logging.info(response)
    return response


def get_gcp_bearer_token() -> str:
    # creds.valid is False, and creds.token is None
    # Need to refresh credentials to populate those
    creds, project = google.auth.default()
    creds.refresh(google.auth.transport.requests.Request())
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def schedule_pipeline(
    project_id: str,
    region: str,
    pipeline_name: str,
    pipeline_template_uri: str,
    pipeline_sa: str,
    pipeline_root: str,
    cron: str,
    max_concurrent_run_count: int,
    pipeline_parameters: dict = None,
    start_time: str = None,
    end_time: str = None,
) -> PipelineJobSchedule:
    pjob = PipelineJob(
        project=project_id,
        template_path=pipeline_template_uri,
        pipeline_root=pipeline_root,
        display_name=f"{pipeline_name}",
        parameter_values=pipeline_parameters,
        labels={"vai-mlops": f"{pipeline_name}"},
    )

    pipeline_job_schedule = pjob.create_schedule(
        display_name=pipeline_name,
        cron=cron,
        max_concurrent_run_count=max_concurrent_run_count,
        service_account=pipeline_sa,
        start_time=start_time,
        end_time=end_time,
    )

    logging.info(f"scheduler for {pipeline_name} submitted")
    return pipeline_job_schedule


def get_schedules(project_id: str, region: str, pipeline_name: str) -> list:
    filter = ""
    if pipeline_name is not None:
        filter = f"filter=display_name={pipeline_name}"
    url = f"https://{region}-aiplatform.googleapis.com/v1/projects/{project_id}/locations/{region}/schedules?{filter}"

    headers = requests.structures.CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = "Bearer {}".format(get_gcp_bearer_token())

    resp = requests.get(url=url, headers=headers)
    data = resp.json()  # Check the JSON Response Content documentation below
    if "schedules" in data:
        return data["schedules"]
    else:
        return None


def pause_schedule(project_id: str, region: str, pipeline_name: str) -> list:
    schedules = get_schedules(project_id, region, pipeline_name)
    if schedules is None:
        logging.info(f"No schedules found with display_name {pipeline_name}")
        return None

    headers = requests.structures.CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = "Bearer {}".format(get_gcp_bearer_token())

    paused_schedules = []
    for s in schedules:
        url = f"https://{region}-aiplatform.googleapis.com/v1/{s['name']}:pause"
        resp = requests.post(url=url, headers=headers)

        data = resp.json()  # Check the JSON Response Content documentation below
        print(resp.status_code == 200)
        if resp.status_code != 200:
            raise Exception(
                f"Unable to pause resourse {s['name']}. request returned with status code {resp.status_code}"
            )
        logging.info(f"scheduled resourse {s['name']} paused")
        paused_schedules.append(s["name"])

    return paused_schedules


def delete_schedules(project_id: str, region: str, pipeline_name: str) -> list:
    schedules = get_schedules(project_id, region, pipeline_name)
    if schedules is None:
        logging.info(f"No schedules found with display_name {pipeline_name}")
        return None

    headers = requests.structures.CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = "Bearer {}".format(get_gcp_bearer_token())

    deleted_schedules = []
    for s in schedules:
        url = f"https://{region}-aiplatform.googleapis.com/v1/{s['name']}"
        resp = requests.delete(url=url, headers=headers)

        data = resp.json()  # Check the JSON Response Content documentation below
        logging.info(f"scheduled resource {s['name']} deleted")
        deleted_schedules.append(s["name"])

    return deleted_schedules


def run_pipeline(
    pipeline_root: str,
    template_path: str,
    project_id: str,
    location: str,
    service_account: str,
    pipeline_parameters: Optional[Dict[str, Any]],
    enable_caching: bool = False,
    experiment_name: str = None,
    job_id: str = None,
    failure_policy: str = "fast",
    labels: Optional[Dict[str, str]] = None,
    credentials: Optional[credentials.Credentials] = None,
    encryption_spec_key_name: Optional[str] = None,
    wait: bool = False,
) -> PipelineJob:
    logging.info(f"Pipeline parameters : {pipeline_parameters}")

    pl = PipelineJob(
        display_name="na",  # not needed and will be optional in next major release
        template_path=template_path,
        job_id=job_id,
        pipeline_root=pipeline_root,
        enable_caching=enable_caching,
        project=project_id,
        location=location,
        parameter_values=pipeline_parameters,
        # input_artifacts=input_artifacts,
        encryption_spec_key_name=encryption_spec_key_name,
        credentials=credentials,
        failure_policy=failure_policy,
        labels=labels,
    )

    pl.submit(service_account=service_account, experiment=experiment_name)
    if wait:
        pl.wait()
        if pl.has_failed:
            raise RuntimeError("Pipeline execution failed")
    return pl
