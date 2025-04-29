import os
import json
import pickle

import argparse
import logging
from google.cloud import bigquery

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

logging.getLogger().setLevel(logging.INFO)
logging.info(f"GCS Project ID: {os.environ.get('CLOUD_ML_PROJECT_ID', None)}")


def _gcsfuse(dpath):
    gs_prefix = "gs://"
    gcsfuse_prefix = "/gcs/"
    if args.model_dir.startswith(gs_prefix):
        dpath = dpath.replace(gs_prefix, gcsfuse_prefix)
        dirpath = os.path.split(dpath)[0]
        if not os.path.isdir(dirpath):
            os.makedirs(dirpath)

    return dpath


def run_training(args):
    bqc = bigquery.Client(project=os.environ.get("CLOUD_ML_PROJECT_ID", None))

    # load training data
    df = bqc.query(query=f"SELECT * FROM `{args.bq_training_table_id}`").to_dataframe()
    X_tr, y_tr = (
        df[df.data_split == "TRAIN"].drop(columns=["data_split", "label"]),
        df["label"][df.data_split == "TRAIN"].astype(int),
    )
    X_ev, y_ev = (
        df[df.data_split == "EVAL"].drop(columns=["data_split", "label"]),
        df["label"][df.data_split == "EVAL"].astype(int),
    )
    X_te, y_te = (
        df[df.data_split == "TEST"].drop(columns=["data_split", "label"]),
        df["label"][df.data_split == "TEST"].astype(int),
    )

    logging.info(
        f"Train dataset: {len(X_tr)}; Eval dataset: {len(X_ev)}; Test dataset: {len(X_te)}"
    )

    n_feats = len(X_tr.columns)
    model = Pipeline(
        [
            (
                "passthru",
                ColumnTransformer(
                    transformers=[  # skip / user_pseudo_id, session_id, date, session_start_tstamp, session_end_tstamp
                        ("pass", "passthrough", list(range(5, n_feats)))
                    ]
                ),
            ),
            ("model", RandomForestClassifier(random_state=42)),
        ]
    )
    model.fit(X_tr, y_tr)

    y_ev_p = model.predict_proba(X_ev)[:, 1]
    y_te_p = model.predict_proba(X_te)[:, 1]

    logging.info(
        f"Eval dataset score: {round(average_precision_score(y_ev, y_ev_p), 4)}"
    )
    logging.info(
        f"Test dataset score: {round(average_precision_score(y_te, y_te_p), 4)}"
    )

    # export all model artifacts into args.model_dir folder
    # in this case we only need model.pkl to be stored away to later recreate the full model
    gcs_model_path = os.path.join(args.model_dir, "model.pkl")
    logging.info("Saving model artifacts to {}".format(gcs_model_path))
    with open(gcs_model_path, "wb") as pickle_file:
        pickle.dump(model, pickle_file)


def run_eval(args):
    bqc = bigquery.Client(project=os.environ.get("CLOUD_ML_PROJECT_ID", None))

    df = bqc.query(
        query=f"SELECT * EXCEPT(data_split) FROM `{args.bq_training_table_id}`"
        f" WHERE data_split = 'TEST'"
    ).to_dataframe()

    X_te, y_te = (
        df.drop(columns=["label"]),
        df["label"].astype(int),
    )

    model = None
    gcs_model_path = os.path.join(args.model_dir, "model.pkl")
    with open(gcs_model_path, "rb") as pickle_file:
        model = pickle.load(pickle_file)

    y_te_p = model.predict_proba(X_te)[:, 1]

    logging.info("Saving metrics to {}metrics.json".format(args.model_dir))

    # all relevant metrics should be stored to args.model_dir folder
    # the file name should be metrics.json
    gcs_metrics_path = os.path.join(args.model_dir, "metrics.json")
    with open(gcs_metrics_path, "w") as f:
        json.dump(
            {
                "eval_metric_name": "average_precision_score",  # required / should point to the metrics used to pick best model
                "average_precision_score": average_precision_score(y_te, y_te_p),
                "roc": roc_auc_score(y_te, y_te_p),  # any other str:float can be added
            },
            f,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model-dir",
        dest="model_dir",
        default=os.getenv("AIP_MODEL_DIR"),
        type=str,
        help="GCS location where all model artifacts are stored. Ex.: model_dir/model.pkl to load picked model.",
    )
    parser.add_argument(
        "--mode",
        dest="mode",
        type=str,
        help="TRAINING or EVAL",
    )
    parser.add_argument(
        "--bq-training-table-id",
        dest="bq_training_table_id",
        type=str,
        help="BQ training table",
    )
    parser.add_argument(
        "--model-id",
        dest="model_id",
        type=str,
        help="Vertex Model ID (model@version). Only populated when mode=EVAL",
    )

    args = parser.parse_args()
    logging.info(f"Model DIR: {args.model_dir}")
    args.model_dir = _gcsfuse(args.model_dir)  # for smooth local/cloud tests
    logging.info(f"Model DIR (GCS Fuse): {args.model_dir}")

    if args.mode == "TRAINING":
        run_training(args)

    elif args.mode == "EVAL":
        run_eval(args)

    else:
        logging.warning(f"Mode not recognized (mode={args.mode})")
