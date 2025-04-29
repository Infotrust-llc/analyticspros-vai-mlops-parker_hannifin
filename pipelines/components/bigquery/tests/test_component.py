import pytest, yaml
from kfp.dsl import Artifact
from pytest_mock import MockerFixture

from pipelines.components.bigquery.component import *


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


def test_bq_call_create_dataset_op(config):
    mock = mock = MockerFixture(config=None)
    destination_table = mock.Mock(
        spec=Dataset,
        # uri=os.path.join(artifacts_path,"artifact"),
        # path=os.path.join(artifacts_path,"artifact"),
        metadata={},
    )

    bq_call_create_dataset_op.python_func(
        project=config["gcp_project_id"],
        run_id="123",
        dataset_id=config["bq_dataset_id"],
        p_mode="TRAINING",  # "INFERENCE",  # "TRAINING",
        p_date_start="2021-01-01",
        p_date_end="2022-01-01",
        training_table=destination_table,
        inference_table=destination_table,
    )


def test_bqml_training_op(config):
    mock = mock = MockerFixture(config=None)
    training_table = mock.Mock(
        spec=Dataset, metadata={"table_id": f"{config['bq_dataset_id']}.training_123"}
    )
    model = mock.Mock(spec=Model)

    bqml_training_op.python_func(
        project=config["gcp_project_id"],
        run_id="123",
        dataset_id=config["bq_dataset_id"],
        training_table=training_table,
        create_model_params=config["model"]["create_model_params"],
        model=model,
    )


def test_bqml_list_models_op(config):
    mock = mock = MockerFixture(config=None)
    models = mock.Mock(spec=Artifact, metadata={})

    models = bqml_list_models_op.python_func(
        project=config["gcp_project_id"],
        dataset_id=config["bq_dataset_id"],
        # models=models
    )

    print(models)


def test_bqml_model_evaluate_op(config):
    mock = mock = MockerFixture(config=None)
    model = {
        "model_name": "model_123",
        "dataset_id": config["bq_dataset_id"],
        "model_id": f"{config['gcp_project_id']}.{config['bq_dataset_id']}.model_123",
    }
    training_table = mock.Mock(
        spec=Dataset, metadata={"table_id": f"{config['bq_dataset_id']}.training_123"}
    )
    metrics = mock.Mock(spec=Metrics)

    bqml_model_evaluate_op.python_func(
        project=config["gcp_project_id"],
        run_id="123",
        model=model,
        training_table=training_table,
        metrics=metrics,
    )


def test_bqml_model_cleanup_op(config):
    mock = mock = MockerFixture(config=None)
    model = mock.Mock(spec=Model)
    metrics = mock.Mock(spec=Metrics)

    bqml_model_cleanup_op.python_func(
        project=config["gcp_project_id"],
        dataset_id=config["bq_dataset_id"],
        keep_n_best_models=None,
        best_model=model,
        metrics=metrics,
    )


def test_bq_calc_percentile_map_op(config):
    mock = mock = MockerFixture(config=None)
    model = mock.Mock(
        spec=Model,
        metadata={
            "model_name": "model_123",
            "dataset_id": config["bq_dataset_id"],
            "model_id": f"{config['gcp_project_id']}.{config['bq_dataset_id']}.model_123",
        },
    )
    training_table = mock.Mock(
        spec=Dataset, metadata={"table_id": f"{config['bq_dataset_id']}.training_123"}
    )
    percentile_map_table = mock.Mock(spec=Dataset, metadata={})

    bq_calc_percentile_map_op.python_func(
        project=config["gcp_project_id"],
        dataset_id=config["bq_dataset_id"],
        run_id="123",
        model=model,
        training_table=training_table,
        percentile_map_table=percentile_map_table,
    )


def test_bqml_predict_op(config):
    mock = mock = MockerFixture(config=None)
    model = mock.Mock(
        spec=Model,
        metadata={
            "model_name": "model_123",
            "dataset_id": config["bq_dataset_id"],
            "model_id": f"{config['gcp_project_id']}.{config['bq_dataset_id']}.model_123",
        },
    )
    inference_table = mock.Mock(
        spec=Dataset, metadata={"table_id": f"{config['bq_dataset_id']}.inference_123"}
    )
    predictions_table = mock.Mock(spec=Dataset, metadata={})

    bqml_predict_op.python_func(
        project=config["gcp_project_id"],
        dataset_id=config["bq_dataset_id"],
        run_id="123",
        inference_table=inference_table,
        model=model,
        predictions_table=predictions_table,
    )
