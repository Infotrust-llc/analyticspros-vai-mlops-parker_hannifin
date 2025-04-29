import os

from schema import Schema, And, Optional, Use, SchemaError


# HELPERS


def val_starts_with_g(x):
    return x.startswith("G-") or x == "secret-manager"


def val_greater_or_equal_to_zero(x):
    return x >= 0


def val_greater_or_equal_to_one(x):
    return x >= 1


def between_5_and_100(x):
    return 5 <= x <= 100


def length_less_than_64(x):
    return len(x) < 64


# START: ACTIVATION

schema_activation_ga4mp = Schema(
    {
        "query_path": And(str, os.path.exists),
        "ga4_measurement_id": And(str, val_starts_with_g),
        "ga4_property_id": int,
        "ga4_api_secret": str,
        Optional("ga4_mp_debug", default=True): bool,
        Optional("create_ga4_setup", default=False): bool,
    }
)

schema_activation_bq_routine = Schema(
    {
        "dataset_id": And(str, length_less_than_64),
        "routine_id": And(str, length_less_than_64),
        Optional("params"): dict,
    }
)

schema_activation = Schema(
    {
        Optional("ga4mp"): schema_activation_ga4mp,
        Optional("bq_routine"): schema_activation_bq_routine,
    }
)

# END: ACTIVATION

# START: MODEL

schema_model = Schema(
    {
        "type": And(str, Use(str.upper), lambda t: t in ["BQML", "CUSTOM"]),
        Optional("create_model_params"): {  # applies to type=BQML
            Optional("model_type", default="BOOSTED_TREE_CLASSIFIER"): And(
                str, Use(str.lower)
            ),
            Optional("hparam_tuning_objectives", default=["ROC_AUC"]): list,
            Optional("num_trials", default=10): And(int, between_5_and_100),
            Optional(str): object,
        },
        Optional("custom_training_params"): {  # applies to type=CUSTOM
            "container_uri": str,
            "model_serving_container_image_uri": str,
            Optional(str): object,
        },
    }
)

# END: MODEL

# START: PREDICTION

schema_prediction = Schema(
    {"cron": str, "data_date_start_days_ago": And(int, val_greater_or_equal_to_zero)}
)

# END: PREDICTION

# START: TRAINING

schema_training = Schema(
    {
        "cron": str,
        "data_date_start_days_ago": And(int, val_greater_or_equal_to_zero),
        "keep_n_best_models": And(int, val_greater_or_equal_to_one),
    }
)

# END: TRAINING

# START: MAIN

config_schema = Schema(
    {
        "gcp_project_id": str,
        "gcp_region": str,
        "bq_dataset_id": And(str, length_less_than_64),
        "bq_stored_procedure_path": And(str, os.path.exists),
        "model": schema_model,
        Optional("training"): schema_training,
        "prediction": schema_prediction,
        Optional("activation"): schema_activation,
        Optional(object): dict,
    }
)

# END: MAIN
