import os
import yaml
from decimal import Decimal

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from activation.ga4mp.main import TransformToPayload


@pytest.fixture()
def config():
    config_file_name = (
        os.environ["CONFIG_FILE_NAME"]
        if "CONFIG_FILE_NAME" in os.environ
        else "config.yaml"
    )
    config_file_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), f"../../{config_file_name}")
    )
    if os.path.exists(config_file_path):
        with open(config_file_path, encoding="utf-8") as fh:
            return yaml.full_load(fh)


def test_transform_to_payload_ep():
    INPUT = [
        {
            "client_id": "client-id-value-test",
            "user_id": "123",
            "event_timestamp": "2023-02-25",
            "event_name": "p_prop",
            "ep_string_field": "string value",
            "ep_int_field": 42,
            "ep_bool_field": True,
            "ep_null_field": None,
            "ep_float_field": 22.4,
        }
    ]

    with TestPipeline() as p:
        input = p | beam.Create(INPUT)
        output = input | beam.ParDo(TransformToPayload())

        assert_that(
            output,
            equal_to(
                [
                    {
                        "client_id": "client-id-value-test",
                        "timestamp_micros": 1677283200000000,
                        "nonPersonalizedAds": False,
                        "events": [
                            {
                                "name": "p_prop",
                                "params": {
                                    "string_field": "string value",
                                    "int_field": 42,
                                    "bool_field": True,
                                    "float_field": 22.4,
                                },
                            }
                        ],
                        "user_id": "123",
                    }
                ]
            ),
        )


def test_transform_to_payload_up():
    INPUT = [
        {
            "client_id": "client-id-value-test",
            "event_timestamp": "2023-02-25",
            "event_name": "p_prop",
            "up_string_field": "string value",
            "up_int_field": 42,
            "up_bool_field": True,
            "up_null_field": None,
            "up_float_field": 22.4,
        }
    ]

    with TestPipeline() as p:
        input = p | beam.Create(INPUT)
        output = input | beam.ParDo(TransformToPayload())

        assert_that(
            output,
            equal_to(
                [
                    {
                        "client_id": "client-id-value-test",
                        "timestamp_micros": 1677283200000000,
                        "nonPersonalizedAds": False,
                        "events": [{"name": "p_prop", "params": {}}],
                        "user_properties": {
                            "string_field": {"value": "string value"},
                            "int_field": {"value": 42},
                            "bool_field": {"value": True},
                            "float_field": {"value": 22.4},
                        },
                    }
                ]
            ),
        )


def test_transform_to_payload_user_data_single_values():
    INPUT = [
        {
            "client_id": "client-id-value-test",
            "event_timestamp": "2023-02-25",
            "event_name": "p_prop",
            "user_data": {
                "sha256_email_address": "yourEmailSha256Variable",
                "sha256_phone_number": "yourPhoneSha256Variable",
                "address": {
                    "sha256_first_name": "yourFirstNameSha256Variable",
                    "sha256_last_name": "yourLastNameSha256Variable",
                    "sha256_street": "yourStreetAddressSha256Variable",
                    "city": "yourCityVariable",
                    "region": "yourRegionVariable",
                    "postal_code": "yourPostalCodeVariable",
                    "country": "yourCountryVariable",
                },
            },
        }
    ]

    with TestPipeline() as p:
        input = p | beam.Create(INPUT)
        output = input | beam.ParDo(TransformToPayload())

        assert_that(
            output,
            equal_to(
                [
                    {
                        "client_id": "client-id-value-test",
                        "timestamp_micros": 1677283200000000,
                        "nonPersonalizedAds": False,
                        "events": [{"name": "p_prop", "params": {}}],
                        "user_data": {
                            "sha256_email_address": "yourEmailSha256Variable",
                            "sha256_phone_number": "yourPhoneSha256Variable",
                            "address": {
                                "sha256_first_name": "yourFirstNameSha256Variable",
                                "sha256_last_name": "yourLastNameSha256Variable",
                                "sha256_street": "yourStreetAddressSha256Variable",
                                "city": "yourCityVariable",
                                "region": "yourRegionVariable",
                                "postal_code": "yourPostalCodeVariable",
                                "country": "yourCountryVariable",
                            },
                        },
                    }
                ]
            ),
        )


def test_transform_to_payload_user_data_multiple_values():
    INPUT = [
        {
            "client_id": "client-id-value-test",
            "event_timestamp": "2023-02-25",
            "event_name": "p_prop",
            "user_data": {
                "sha256_email_address": ["yourEmailSha256Variable"],
                "sha256_phone_number": ["yourPhoneSha256Variable"],
                "address": [
                    {
                        "sha256_first_name": "yourFirstNameSha256Variable",
                        "sha256_last_name": "yourLastNameSha256Variable",
                        "sha256_street": "yourStreetAddressSha256Variable",
                        "city": "yourCityVariable",
                        "region": "yourRegionVariable",
                        "postal_code": "yourPostalCodeVariable",
                        "country": "yourCountryVariable",
                    },
                    {
                        "sha256_first_name": "yourFirstNameSha256Variable",
                        "sha256_last_name": "yourLastNameSha256Variable",
                        "sha256_street": "yourStreetAddressSha256Variable",
                        "city": "yourCityVariable",
                        "region": "yourRegionVariable",
                        "postal_code": "yourPostalCodeVariable",
                        "country": "yourCountryVariable",
                    },
                ],
            },
        }
    ]

    with TestPipeline() as p:
        input = p | beam.Create(INPUT)
        output = input | beam.ParDo(TransformToPayload())

        assert_that(
            output,
            equal_to(
                [
                    {
                        "client_id": "client-id-value-test",
                        "timestamp_micros": 1677283200000000,
                        "nonPersonalizedAds": False,
                        "events": [{"name": "p_prop", "params": {}}],
                        "user_data": {
                            "sha256_email_address": ["yourEmailSha256Variable"],
                            "sha256_phone_number": ["yourPhoneSha256Variable"],
                            "address": [
                                {
                                    "sha256_first_name": "yourFirstNameSha256Variable",
                                    "sha256_last_name": "yourLastNameSha256Variable",
                                    "sha256_street": "yourStreetAddressSha256Variable",
                                    "city": "yourCityVariable",
                                    "region": "yourRegionVariable",
                                    "postal_code": "yourPostalCodeVariable",
                                    "country": "yourCountryVariable",
                                },
                                {
                                    "sha256_first_name": "yourFirstNameSha256Variable",
                                    "sha256_last_name": "yourLastNameSha256Variable",
                                    "sha256_street": "yourStreetAddressSha256Variable",
                                    "city": "yourCityVariable",
                                    "region": "yourRegionVariable",
                                    "postal_code": "yourPostalCodeVariable",
                                    "country": "yourCountryVariable",
                                },
                            ],
                        },
                    }
                ]
            ),
        )


def test_pipeline(config):
    from main import run

    params = {
        "project": config["gcp_project_id"],
        "source_table": f'{config["gcp_project_id"]}.{config["bq_dataset_id"]}.activation_ga4mp',
        "prediction_run_id": "123",
        "temp_location": f'gs://{config["gcp_project_id"]}-pipelines',
        "ga4_measurement_id": "123",
        "ga4_api_secret": "123",
        "log_db_dataset": {config["bq_dataset_id"]},
        "use_api_validation": 1,
        "direct": "direct",
        # "direct_running_mode": "multi_processing",
        # "direct_num_workers": 2,
    }
    run(params)


def test_pipeline_secret_manager(config):
    from main import run

    params = {
        "project": config["gcp_project_id"],
        "source_table": f'{config["gcp_project_id"]}.{config["bq_dataset_id"]}.activation_ga4mp',
        "prediction_run_id": "123",
        "temp_location": f'gs://{config["gcp_project_id"]}-{config["bq_dataset_id"].replace("_", "-")}-pipelines',
        "ga4_measurement_id": "secret-manager",
        "ga4_api_secret": "ga4-rollup-api-secrets",
        "log_db_dataset": {config["bq_dataset_id"]},
        "use_api_validation": 1,
        "direct": "direct",
        # "direct_running_mode": "multi_processing",
        # "direct_num_workers": 2,
    }
    run(params)
