import argparse
import os
import sys
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

config = json.loads(sys.stdin.read())
try:
    client = bigquery.Client(project=config["gcp_project_id"])
    ds = client.get_dataset(config["bq_dataset_id"])
    if (
        "vai-mlops" in ds.labels
    ):  # dataset was created by vai-mlops / don't try to delete if apply is re-ran
        print(json.dumps({"create_dataset": "1"}))
        sys.exit()

    print(json.dumps({"create_dataset": "0"}))

except NotFound as e:
    print(json.dumps({"create_dataset": "1"}))
