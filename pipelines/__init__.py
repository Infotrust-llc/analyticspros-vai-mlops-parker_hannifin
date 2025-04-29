import os
import yaml

config_file_name = (
    os.environ["CONFIG_FILE_NAME"]
    if "CONFIG_FILE_NAME" in os.environ
    else "config.yaml"
)
config_file_path = os.path.join(os.path.dirname(__file__), f"../{config_file_name}")

config = None
base_image = None
if os.path.exists(config_file_path):
    with open(config_file_path, encoding="utf-8") as fh:
        config = yaml.full_load(fh)

    repo_docker_name = config["bq_dataset_id"].replace("_", "-")
    base_image = f"{config['gcp_region']}-docker.pkg.dev/{config['gcp_project_id']}/{repo_docker_name}/vai-pipelines:prod"

else:
    raise Exception(f"Config not found! [{config_file_path}]")
