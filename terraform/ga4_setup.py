import argparse
import os
import json
import sys

import google.api_core.exceptions
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha import (
    ConversionEvent,
    CustomDimension,
    CreateCustomDimensionRequest,
    ListCustomDimensionsRequest,
    ArchiveCustomDimensionRequest,
)
from sqlglot import parse_one, exp
from jinja2 import Environment, BaseLoader

from helpers import retrieve_config


def create_conversion_event(
    ga4c: AnalyticsAdminServiceClient, property_id: str, event_name: str
):
    conversion_event = ga4c.create_conversion_event(
        parent=f"properties/{property_id}",
        conversion_event=ConversionEvent(
            event_name=event_name, counting_method="ONCE_PER_SESSION"
        ),
    )


def create_custom_dimension(
    ga4c: AnalyticsAdminServiceClient,
    property_id: str,
    field_name: str,
    display_name: str,
    scope: str,
):
    custom_dimension = CustomDimension()
    custom_dimension.parameter_name = field_name
    custom_dimension.display_name = display_name
    custom_dimension.scope = scope

    request = CreateCustomDimensionRequest(
        parent=f"properties/{property_id}",
        custom_dimension=custom_dimension,
    )
    ga4c.create_custom_dimension(request=request)


def create_setup(
    ga4_property_id, event_name: str, event_params: list, user_props: list
):
    ga4c = AnalyticsAdminServiceClient(transport=None)
    try:
        create_conversion_event(ga4c, ga4_property_id, event_name)
        print(f"Conversion event with name `{event_name}` created!")

    except google.api_core.exceptions.AlreadyExists as eae:
        print(f"Conversion event with name `{event_name}` already exists!")

    for name in user_props:
        try:
            create_custom_dimension(
                ga4c, ga4_property_id, field_name=name, display_name=name, scope="USER"
            )
            print(f"User-scoped CD with the name `{name}` created!")

        except google.api_core.exceptions.AlreadyExists as eae:
            print(f"User-scoped CD with the name `{name}` already exists!")

    for name in event_params:
        try:
            create_custom_dimension(
                ga4c, ga4_property_id, field_name=name, display_name=name, scope="EVENT"
            )
            print(f"Event-scoped CD with the name `{name}` created!")

        except google.api_core.exceptions.AlreadyExists as eae:
            print(f"event-scoped CD with the name `{name}` already exists!")


def delete_setup(
    ga4_property_id, event_name: str, event_params: list, user_props: list
):
    ga4c = AnalyticsAdminServiceClient(transport=None)
    try:
        results = ga4c.list_conversion_events(parent=f"properties/{ga4_property_id}")
        for ce in results:
            if ce.event_name == event_name:
                ga4c.delete_conversion_event(name=f"{ce.name}")

        print(f"Conversion event with name `{event_name}` deleted!")

    except google.api_core.exceptions.AlreadyExists as eae:
        print(f"Conversion event with name `{event_name}` already deleted!")

    for name in user_props + event_params:
        try:
            request = ListCustomDimensionsRequest(
                parent=f"properties/{ga4_property_id}"
            )
            results = ga4c.list_custom_dimensions(request=request)
            for cd in results:
                if cd.parameter_name == name:
                    request = ArchiveCustomDimensionRequest(name=cd.name)
                    ga4c.archive_custom_dimension(request=request)

                    print(f"{cd.scope}-scoped CD with the name `{name}` archived!")

        except google.api_core.exceptions.AlreadyExists as eae:
            print(f"User-scoped CD with the name `{name}` already archived!")


def main(args):
    config = retrieve_config(args.config_path)
    if not config.get("activation", {}).get("ga4mp", False):
        sys.exit()

    activation_query_path = config["activation"]["ga4mp"]["query_path"]

    sqlx = None
    with open(activation_query_path, "r") as stream:
        sqlx = stream.read()

    sqlx = Environment(loader=BaseLoader).from_string(sqlx)
    sql = sqlx.render(config)

    sqlparser = parse_one(sql, read="bigquery")

    event_name = None
    event_params = []
    user_props = []
    for col in sqlparser.selects:
        if col.alias == "event_name":
            if not isinstance(col.args["this"], exp.Literal):
                raise Exception(
                    f"event_name column in the activation query is not a literal string!"
                )

            event_name = col.args["this"].alias_or_name

        elif col.alias_or_name.startswith("ep_") and not col.alias_or_name.endswith(
            "_"
        ):
            event_params.append(col.alias[3:])

        elif col.alias_or_name.startswith("up_") and not col.alias_or_name.endswith(
            "_"
        ):
            user_props.append(col.alias[3:])

    print(f"Event name: `{event_name}`")
    print(f"Event params: {event_params}")
    print(f"User props: {user_props}")
    print("---")

    ga4_property_ids = []
    ga4_measurement_id = config["activation"]["ga4mp"]["ga4_measurement_id"]
    if ga4_measurement_id == "secret-manager":
        from google.cloud import secretmanager

        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{config['gcp_project_id']}/secrets/{config['activation']['ga4mp']['ga4_api_secret']}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        streams = json.loads(response.payload.data.decode("UTF-8"))
        ga4_property_ids = [
            metadata["property_id"] for stream_id, metadata in streams.items()
        ]

    else:
        ga4_property_ids.append(config["activation"]["ga4mp"]["ga4_property_id"])

    for ga4_property_id in ga4_property_ids:
        if args.create:
            create_setup(ga4_property_id, event_name, event_params, user_props)

        elif args.delete:
            delete_setup(ga4_property_id, event_name, event_params, user_props)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="GA4 Setup",
        description="Creates a GA4 conversion event and custom dimensions based on the activation query",
        epilog='"That which we persist in doing becomes easier to do, not that the nature of the thing has changed but that our power to do has increased."\n\t-Ralph Waldo Emerson',
    )
    parser.add_argument("--config_path", type=str, required=False)
    parser.add_argument("-c", "--create", action="store_true")
    parser.add_argument("-d", "--delete", action="store_true")
    args = parser.parse_args()
    main(args)
