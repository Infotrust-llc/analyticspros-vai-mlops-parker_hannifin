import pytest
import yaml
from pytest_mock import MockerFixture

from pipelines.components.vertex.component import *


@pytest.fixture()
def config():
    config_file_name = (
        os.environ["CONFIG_FILE_NAME"]
        if "CONFIG_FILE_NAME" in os.environ
        else "config.yaml"
    )
    config_file_path = os.path.join(
        os.path.dirname(__file__), f"../../../../{config_file_name}"
    )
    if os.path.exists(config_file_path):
        with open(config_file_path, encoding="utf-8") as fh:
            return yaml.full_load(fh)


def test_vai_get_default_model_op(config):
    mock = mock = MockerFixture(config=None)
    default_model = mock.Mock(
        spec=Model,
        # uri=os.path.join(artifacts_path,"artifact"),
        # path=os.path.join(artifacts_path,"artifact"),
        metadata={},
    )

    vai_get_default_model_op.python_func(
        project=config["gcp_project_id"],
        region=config["gcp_region"],
        dataset_id=config["bq_dataset_id"],
        default_model=default_model,
    )


def test_vai_custom_training_op(config):
    mock = mock = MockerFixture(config=None)
    model = mock.Mock(
        spec=Model,
        metadata={},
    )

    training_table = mock.Mock(
        spec=Dataset,
        metadata={"table_id": f"{config['bq_dataset_id']}.training_202404262311"},
    )

    vai_custom_training_op.python_func(
        project=config["gcp_project_id"],
        region=config["gcp_region"],
        run_id="202404262311",
        dataset_id=config["bq_dataset_id"],
        training_table=training_table,
        custom_training_params={
            "container_uri": "us-central1-docker.pkg.dev/as-dev-anze/vaimlops-custom-model-custom/vai-training-container:prod",
            "model_serving_container_image_uri": "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
            "replica_count": 1,
            "machine_type": "n1-standard-4",
        },
        model=model,
    )


def test_vai_model_evaluate_op(config):
    mock = mock = MockerFixture(config=None)
    model = {
        "dataset_id": config["bq_dataset_id"],
        "model_id": f"projects/365259031240/locations/us-central1/models/8924605040774086656@2",
    }

    training_table = mock.Mock(
        spec=Dataset,
        metadata={"table_id": f"{config['bq_dataset_id']}.training_123"},
    )

    vai_model_evaluate_op.python_func(
        project=config["gcp_project_id"],
        region=config["gcp_region"],
        run_id="123",
        model=model,
        training_table=training_table,
        custom_training_params={
            "container_uri": "us-central1-docker.pkg.dev/as-dev-anze/vaimlops-custom-model/vai-training-container:prod",
            "model_serving_container_image_uri": "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-2:latest",
            "replica_count": 1,
            "machine_type": "n1-standard-4",
        },
        metrics=mock.Mock(spec=Metrics),
    )


def test_vai_batch_prediction_op(config):
    mock = mock = MockerFixture(config=None)
    inference_table = mock.Mock(
        spec=Dataset,
        metadata={
            "table_id": f"{config['gcp_project_id']}.{config['bq_dataset_id']}.test_123"
        },
    )
    predictions_table = mock.Mock(spec=Dataset, metadata={})
    model = mock.Mock(
        spec=Model,
        metadata={
            "model_id": "projects/365259031240/locations/us-central1/models/8924605040774086656@1"
        },
    )

    vai_batch_prediction_op.python_func(
        project=config["gcp_project_id"],
        dataset_id=config["bq_dataset_id"],
        run_id="123",
        inference_table=inference_table,
        predictions_table=predictions_table,
        model=model,
    )


def test_vai_model_cleanup_op(config):
    mock = mock = MockerFixture(config=None)
    model = mock.Mock(spec=Model)
    metrics = mock.Mock(spec=Metrics)

    vai_model_cleanup_op.python_func(
        project=config["gcp_project_id"],
        dataset_id=config["bq_dataset_id"],
        keep_n_best_models=None,
        best_model=model,
        metrics=metrics,
    )
