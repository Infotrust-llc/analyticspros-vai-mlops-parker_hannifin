import datetime, pytz
import hashlib
import json
import logging
import uuid
from copy import deepcopy
from decimal import Decimal

import apache_beam as beam
import requests
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import GoogleCloudOptions, DirectOptions
from google.cloud import secretmanager


class ActivationOptions(DirectOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--project",
            type=str,
            help="GCP Project Id",
            required=True,
        )

        parser.add_argument(
            "--source_table",
            type=str,
            help="table specification for the source data. Format [dataset.data_table]",
            required=True,
        )
        parser.add_argument(
            "--prediction_run_id",
            type=str,
            help="Run id of the prediction - YYYYMMDDHHmm",
            required=True,
        )

        parser.add_argument(
            "--ga4_measurement_id",
            type=str,
            help="Measurement ID in GA4",
            required=True,
        )
        parser.add_argument(
            "--ga4_api_secret",
            type=str,
            help="Client secret for sending to GA4",
            required=True,
        )
        parser.add_argument(
            "--log_db_dataset",
            type=str,
            help="dataset where log_table is created",
            required=True,
        )
        parser.add_argument(
            "--use_api_validation",
            type=bool,
            help="Use Measurement Protocol API validation for debugging instead of sending the events",
            default=False,
            nargs="?",
        )


class CallMeasurementProtocolAPI(beam.DoFn):
    def __init__(self, project_id, measurement_id, api_secret, debug=False):
        super().__init__()

        self.project_id = project_id
        self.measurement_id = measurement_id
        self.api_secret = api_secret

        self.stream_secrets = {}
        self.debug_str = ""
        if debug:
            self.debug_str = "debug/"

    def _event_post_url(self, measurement_id, api_secret, stream_id):
        if measurement_id != "secret-manager":
            return f"https://www.google-analytics.com/{self.debug_str}mp/collect?measurement_id={measurement_id}&api_secret={api_secret}"

        else:
            mid = self.stream_secrets[stream_id]["measurement_id"]
            sid = self.stream_secrets[stream_id]["api_secret"]
            return f"https://www.google-analytics.com/{self.debug_str}mp/collect?measurement_id={mid}&api_secret={sid}"

    def setup(self):
        if self.measurement_id == "secret-manager":
            client = secretmanager.SecretManagerServiceClient()
            name = (
                f"projects/{self.project_id}/secrets/{self.api_secret}/versions/latest"
            )
            response = client.access_secret_version(request={"name": name})
            self.stream_secrets = json.loads(response.payload.data.decode("UTF-8"))

    def process(self, element):
        stream_id = element.pop("stream_id", None)

        try:
            ga4_mp_url = self._event_post_url(
                self.measurement_id, self.api_secret, stream_id
            )

        except Exception as e:
            yield element, 500, str(e)

        response = requests.post(
            ga4_mp_url,
            data=json.dumps(element),
            headers={"content-type": "application/json"},
            timeout=20,
        )
        yield element, response.status_code, response.content


class ToLogFormat(beam.DoFn):
    def __init__(self, prediction_run_id):
        self.prediction_run_id = prediction_run_id

    def process(self, element):
        if element[1] == requests.status_codes.codes.NO_CONTENT:
            state_msg = "SEND_OK"
        else:
            state_msg = "SEND_FAIL"

        pl = json.dumps(element[0], sort_keys=True)

        yield {
            "prediction_run_ts": datetime.datetime.strptime(
                self.prediction_run_id, "%Y%m%d%H%M"
            ),
            "prediction_run_id": self.prediction_run_id,
            "id": str(uuid.uuid4()),
            "client_id": element[0]["client_id"],
            "event_name": element[0]["events"][0]["name"],
            "payload_md5": hashlib.md5(pl.encode()).hexdigest(),
            "payload": pl,
            "response": element[2],
            "state": f"{state_msg} {element[1]}",
        }


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


class TransformToPayload(beam.DoFn):
    def __init__(self):
        super().__init__()
        self.date_formats = [  # from more specific to less
            "%Y-%m-%d %H:%M:%S.%f UTC",
            "%Y-%m-%d %H:%M:%S UTC",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]
        self.payload_template = None

    def setup(self):
        self.payload_template = {
            "client_id": None,
            "timestamp_micros": None,
            "nonPersonalizedAds": False,
            "events": [{"name": None, "params": {}}],
        }

    def process(self, element):
        hit = deepcopy(self.payload_template)
        hit["client_id"] = element["client_id"]
        if "user_id" in element:
            hit["user_id"] = element["user_id"]
        hit["timestamp_micros"] = self.date_to_micro(element["event_timestamp"])
        hit["events"][0]["name"] = element["event_name"]

        up, ep = self.extract_params(element)

        if up:
            hit["user_properties"] = up

        if ep:
            hit["events"][0]["params"] = ep

        if "user_data" in element:
            hit["user_data"] = element["user_data"]

        if "stream_id" in element:
            hit["stream_id"] = element["stream_id"]

        yield hit

    def date_to_micro(self, date_str):
        ts = None
        for df in self.date_formats:
            try:
                dt = datetime.datetime.strptime(date_str, df)
                dt = dt.replace(tzinfo=pytz.utc)
                ts = int(dt.timestamp() * 1e6)
                break
            except:
                pass

        return ts

    def extract_params(self, element):
        el = element.copy()
        user_props, event_props = {}, {}

        for k, v in el.items():
            k_clean = k[3:-1] if k.endswith("_") else k[3:]
            if k.startswith("up_") and v is not None:
                user_props[k_clean] = {"value": v}

            elif k.startswith("ep_") and v is not None:
                event_props[k_clean] = v

        return user_props, event_props


def run(params=None):
    if params is not None:
        pipeline_options = DirectOptions.from_dictionary(params)
    else:
        pipeline_options = DirectOptions()

    activation_options = pipeline_options.view_as(ActivationOptions)

    log_table_name = f"activation_ga4mp_log"
    table_schema = {
        "fields": [
            {"name": "prediction_run_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "prediction_run_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "client_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "event_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payload_md5", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payload", "type": "STRING", "mode": "REQUIRED"},
            {"name": "response", "type": "STRING", "mode": "REQUIRED"},
            {"name": "state", "type": "STRING", "mode": "REQUIRED"},
        ]
    }

    log_table_spec = bigquery.TableReference(
        projectId=activation_options.project,
        datasetId=activation_options.log_db_dataset,
        tableId=log_table_name,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        measurement_api_responses = (
            p
            | beam.io.gcp.bigquery.ReadFromBigQuery(
                project=activation_options.project,
                query=f"SELECT * FROM `{activation_options.source_table}`",
                use_json_exports=True,
                use_standard_sql=True,
            )
            | "Transform to Measurement Protocol API payload"
            >> beam.ParDo(TransformToPayload())
            | "POST event to Measurement Protocol API"
            >> beam.ParDo(
                CallMeasurementProtocolAPI(
                    activation_options.project,
                    activation_options.ga4_measurement_id,
                    activation_options.ga4_api_secret,
                    debug=activation_options.use_api_validation,
                )
            )
        )

        _ = (
            measurement_api_responses
            | "Transform log format"
            >> beam.ParDo(ToLogFormat(activation_options.prediction_run_id))
            | "Store to log table"
            >> beam.io.WriteToBigQuery(
                log_table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                additional_bq_parameters={
                    "timePartitioning": {"type": "DAY", "field": "prediction_run_ts"},
                    "clustering": {"fields": ["state", "client_id"]},
                },
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
