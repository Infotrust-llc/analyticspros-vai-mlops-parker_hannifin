import pytest, yaml
from pytest_mock import MockerFixture
from jinja2 import Environment, BaseLoader

from pipelines.components.activation.component import *


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


def test_activation_ga4mp_op(config):
    mock = mock = MockerFixture(config=None)
    activation_log_table = mock.Mock(spec=Dataset, metadata={})

    sqlx = None
    qpath = os.path.join(
        os.path.dirname(__file__),
        f"../../../../{config['activation']['ga4mp']['query_path']}",
    )
    with open(qpath, "r") as stream:
        sqlx = stream.read()

    sqlx = Environment(loader=BaseLoader).from_string(sqlx)
    sql = sqlx.render(config)

    activation_ga4mp_op.python_func(
        project=config["gcp_project_id"],
        dataset_id=config["bq_dataset_id"],
        run_id="202309172113",
        query=sql,
        ga4_measurement_id=config["activation"]["ga4mp"]["ga4_measurement_id"],
        ga4_api_secret=config["activation"]["ga4mp"]["ga4_api_secret"],
        activation_log_table=activation_log_table,
    )


def test_activation_bq_routine_op(config):
    mock = mock = MockerFixture(config=None)
    activation_log_table = mock.Mock(spec=Dataset, metadata={})

    activation_bq_routine_op.python_func(
        project=config["gcp_project_id"],
        dataset_id=config["bq_dataset_id"],
        run_id="202309172113",
        cfg=config["activation"]["bq_routine"],
    )
