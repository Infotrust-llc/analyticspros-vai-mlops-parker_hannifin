import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
from terraform.helpers import retrieve_config, validate_config
import jinja2
import yaml
import pandas as pd
from warnings import simplefilter
from google.cloud import bigquery
from common.retry_policies import *


simplefilter(action="ignore", category=FutureWarning)
import re


class MissingConfig(Exception):
    pass


class ConfigError(Exception):
    pass


class FeatureFinder:
    """
    Uses a query to generate features from URLS based on the amount of traffic.
    """

    def __init__(
        self,
        billing_project_id: str,
        region: str,
        src_project_id: str,
        src_dataset_id: str,
        dst_dataset_id: str,
        date_start: str,
        date_end: str,
        re_page_path: str,
        perc_keep: float,
        min_engagement: int,
        template_file: str = "sp_create_dataset.sqlx.tpl",
        destination_file: str = "sp_create_dataset.sqlx",
        kwargs: dict = {},
    ):
        self.billing_project_id = billing_project_id
        self.region = region
        self.src_project_id = src_project_id
        self.src_dataset_id = src_dataset_id
        self.dst_dataset_id = dst_dataset_id
        self.date_start = date_start
        self.date_end = date_end
        self.re_page_path = re_page_path
        self.perc_keep = perc_keep
        self.min_engagement = min_engagement
        self.template_file = template_file
        self.destination_file = destination_file
        self.kwargs = kwargs

    def _column_name_clean(self, f: str) -> str:
        """
        Turns column names (taken from URLs) into legible features

        Ex. Turns "/" into "homepage"
        Ex. Turns "/page_path/" into page_path

        Parameters
        ----------
        f : str
            The name of the feature we want to clean up

        Returns
        -------
        str
            The cleaned string
        """
        if f == "/" or f == "" or f is None:
            return "homepage"
        if f.startswith("/"):
            f = f[1:]
        if f.endswith("/"):
            f = f[:-1]
        return re.sub("[^0-9a-zA-Z]+", "_", f)

    def render_features_to_sql(self, df_features: pd.DataFrame) -> None:
        """
        Takes a Jinja-formatted .sqlx file and formats it using the features and values from our system.

        Variables will be filled from our config/arguments/keyword arguments, features will be created for every row in our dataframe.

        Parameters
        ----------
        df_features : pd.DataFrame
            a dataframe where every row represents a feature we want to include in our SQL query.
        """
        print(f"Rendering new sql query to file: {self.destination_file}.")
        # Create a dict that has a key/value pair for every pice of information this system has received
        jinja_config = vars(self)
        jinja_config["features"] = df_features

        # Fill the variables in the Jinja template
        env = jinja2.Environment(loader=jinja2.FileSystemLoader("./"))
        template = env.get_template(self.template_file)
        output = template.render(**jinja_config)
        with open(self.destination_file, "w") as f:
            f.write(output)

    def find_features(self) -> pd.DataFrame:
        """
        Uses a SQL to generate features for the AnDY system based on the amount of traffic that passes through URLs.

        Returns
        -------
        pd.DataFrame
            a dataframe where every row represents a valuable URL to be used as a feature.
        """
        print("Attempting to find features.")
        client = bigquery.Client(project=self.billing_project_id)
        sql = f"""
        SELECT
            feature,
            ROUND(100 * SUM(users) OVER (ORDER BY users DESC) / SUM(users) OVER (), 2) as cumulative_traffic_percent,

        FROM (
            SELECT
                REGEXP_EXTRACT(page_path, @RE_PAGE_PATH) as feature,
                COUNT(DISTINCT user_id) as users

            FROM (
                SELECT
                    user_pseudo_id as user_id,
                    (SELECT value.string_value FROM UNNEST(event_params) as ep WHERE ep.key = 'page_location') as page_path
                FROM `{self.src_project_id}.{self.src_dataset_id}.events_*`
                WHERE
                    event_name = 'page_view'
                    AND SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN @DATE_START AND @DATE_END
            )
            GROUP BY 1
        )
        WHERE
            feature IS NOT NULL
        QUALIFY
            cumulative_traffic_percent <= @PERC_KEEP
        ORDER BY 2 ASC
        """

        print("Running query.")
        df_features = client.query(
            query=sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "DATE_START", "DATE", self.date_start
                    ),
                    bigquery.ScalarQueryParameter("DATE_END", "DATE", self.date_end),
                    bigquery.ScalarQueryParameter(
                        "RE_PAGE_PATH", "STRING", self.re_page_path
                    ),
                    bigquery.ScalarQueryParameter(
                        "PERC_KEEP", "FLOAT64", self.perc_keep
                    ),
                ]
            ),
            job_retry=BIGQUERY_RETRY_POLICY
        ).to_dataframe()

        print(f"Number of page path categories kept: {len(df_features)}")

        # scrub the feature names to make them "pretty"
        df_features["feature_name"] = df_features.feature.apply(self._column_name_clean)

        return df_features


def main(args):
    # If we have a config, fill values from the config.yaml file
    if args.config_filename:
        print("Running from configuration file.")
        config = retrieve_config(args.config_filename)
        billing_project_id = config["gcp_project_id"]
        region = config["gcp_region"]
        src_project_id = "bigquery-public-data"
        src_dataset_id = "ga4_obfuscated_sample_ecommerce"
        dst_dataset_id = config["bq_dataset_id"]
        date_start = "2020-12-01"
        date_end = "2020-12-15"
        re_page_path = config["bq_sp_params"]["re_page_path"]
        perc_keep = config["bq_sp_params"]["perc_keep"]
        min_engagement = config["bq_sp_params"]["min_engagement"]
        template_file = f"{config['bq_stored_procedure_path']}.tpl"
        destination_file = f"{config['bq_stored_procedure_path']}"
        kwargs = args.__dict__.update(config)

    # if we do not have a config, fill the values from command-line arguments
    else:
        print("Running from command line arguments.")
        billing_project_id = args.billing_project_id
        region = args.region
        src_project_id = args.src_project_id
        src_dataset_id = args.src_dataset_id
        dst_dataset_id = args.dst_dataset_id
        date_start = args.date_start
        date_end = args.date_end
        re_page_path = args.re_page_path
        perc_keep = args.perc_keep
        min_engagement = args.min_engagement
        template_file = args.template_file
        destination_file = args.destination_file
        kwargs = args.__dict__

    ff = FeatureFinder(
        billing_project_id,
        region,
        src_project_id,
        src_dataset_id,
        dst_dataset_id,
        date_start,
        date_end,
        re_page_path,
        perc_keep,
        min_engagement,
        template_file,
        destination_file,
        kwargs,
    )

    features = ff.find_features()
    print("Features found:\n", features)

    if args.render:
        ff.render_features_to_sql(features)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Site Path Feature Finder",
        description="Analyzes GA4 traffic patterns to find website URLs that could be valuable features for ML modeling.",
        epilog='"That which we persist in doing becomes easier to do, not that the nature of the thing has changed but that our power to do has increased."\n\t-Ralph Waldo Emerson',
    )
    parser.add_argument(
        "--config_filename",
        help="Location of a config.yaml file. If this is provided all variables will be retrieved from the config.",
    )
    parser.add_argument(
        "--billing_project_id",
        help="The GCP project id that will be used as a billing project for the system.",
    )
    parser.add_argument("--region", default="us-central1", help="The GCP region.")
    parser.add_argument(
        "--src_project_id",
        help="The project that holds the data we would like to query. If left blank, will default to the billing_project_id argument",
    )
    parser.add_argument(
        "--src_dataset_id",
        help="The dataset that holds the data we would like to query.",
    )
    parser.add_argument(
        "--dst_dataset_id",
        help="The dataset in which our create_dataset routine will be added.",
    )
    parser.add_argument(
        "--date_start",
        help="The start date for which we would like to analyze data in order to generate URL features. Must be in the format YYYY-mm-dd",
    )
    parser.add_argument(
        "--date_end",
        help="The end date for which we would like to analyze data in order to generate URL features. Must be in the format YYYY-mm-dd",
    )
    parser.add_argument(
        "--re_page_path",
        help="Regex to extract the query string of a URL. Ex: https://shop.googlemerchandisestore.com(/[a-z-0-9]*/?).* ",
    )
    parser.add_argument(
        "--perc_keep",
        help="The amount of traffic that we would like to analyze for our features. Ex: If set to 95, we will analyze the most visited 95% of URLs to generate features",
    )
    parser.add_argument(
        "--min_engagement",
        help="The minumum amount of times a URL should be visited to be considered a feature.",
    )
    parser.add_argument(
        "--template_file",
        default="sp_create_dataset.sqlx.tpl",
        help="A jinja formatted file that will be dynamically filled with variables",
    )
    parser.add_argument(
        "--destination_file",
        default="sp_create_dataset.sqlx",
        help="The location that we would like to save the finished SQL file.",
    )
    parser.add_argument(
        "--kwargs",
        nargs="+",
        metavar="KEY=VALUE",
        help="Key/Value pairs that need to be provided to fill out the Jinja template file. Ex: --kv foo=bar key=value abc=123",
    )
    parser.add_argument(
        "--render",
        action="store_true",
        help="If true, generates our dynamically populated SQL file at the destination_file location.",
    )

    args = parser.parse_args()

    # argument specific logic that enables "defaulting" to other values
    if args.src_project_id is None:
        args.src_project_id = args.billing_project_id

    if args.dst_dataset_id is None:
        args.dst_dataset_id = args.src_dataset_id

    # Make our kwargs their own variables in our args dictionary for convenience
    kwargs = {}
    if args.kwargs:
        for pair in args.kwargs:
            key, value = pair.split("=")
            kwargs[key] = value

    args.__dict__.update(kwargs)

    main(args)
