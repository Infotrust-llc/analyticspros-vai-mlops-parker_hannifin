import argparse
import json

from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import CreateMeasurementProtocolSecretRequest
from google.auth import default


def get_property(client, property_id):
    try:
        return client.get_property(name=property_id)
    except Exception as e:
        print(
            f"Error retrieving property '{property_id}': {e} or property does not exist."
        )
        return None


def is_rollup_property(property):
    # enum 3 represents a rollup property
    return property and property.property_type == 3


def list_or_create_secrets(client, stream_id):
    existing_secrets = client.list_measurement_protocol_secrets(parent=stream_id)
    if list(existing_secrets):
        return existing_secrets
    else:
        secret_request = CreateMeasurementProtocolSecretRequest(
            parent=stream_id, measurement_protocol_secret={"display_name": f"vai-mlops"}
        )
        return [client.create_measurement_protocol_secret(request=secret_request)]


def process_streams(client, property, secrets_info):
    data_streams = client.list_data_streams(parent=property)
    property_id = property.split("/")[-1]
    for stream in data_streams:
        stream_id = stream.name.split("/")[-1]
        secrets = list_or_create_secrets(client, stream.name)
        for secret in secrets:
            secrets_info[stream_id] = {
                "property_id": property_id,
                "measurement_id": stream.web_stream_data.measurement_id,
                "api_secret": secret.secret_value,
            }
            break


def generate_secrets_info(client, property_id):
    secrets_info = {}
    property = get_property(client, property_id)

    if not property:
        return secrets_info

    if is_rollup_property(property):
        print(f"Processing rollup property '{property_id}'...")
        source_links = client.list_rollup_property_source_links(parent=property_id)
        for source_link in source_links:
            process_streams(client, source_link.source_property, secrets_info)
    else:
        return f"Property {property_id} is not a rollup property."

    return secrets_info


def write_to_json(secrets_info, output_file=None):
    output_json = json.dumps(secrets_info, indent=2)
    if output_file:
        with open(output_file, "w") as file:
            file.write(output_json)
    else:
        print(output_json)


def main(property_id, output_file):
    credentials, _ = default()
    client = AnalyticsAdminServiceClient(credentials=credentials)
    secrets_info = generate_secrets_info(client, property_id)
    write_to_json(secrets_info, output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create GA4 API secrets for data streams."
    )
    parser.add_argument("--property_id", type=str, help="GA4 property ID")
    parser.add_argument("--output", type=str, help="Output file name", default=None)
    args = parser.parse_args()
    main(f"properties/{args.property_id}", args.output)
