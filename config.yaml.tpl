gcp_project_id: your-project-id  # GCP Project Id where the pipelines and models will live
gcp_region: us-central1
bq_dataset_id: test_propensity  # BQ dataset name where all models and tables will live
bq_stored_procedure_path: sp_create_dataset.sqlx  # Path to the create_dataset procedure
bq_sp_params:  # Custom create_dataset parameters
    lookback: 14
    lookahead: 7

model:
    type: BQML  # BQML or CUSTOM
    create_model_params:  # https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-boosted-tree#create_model
        model_type: BOOSTED_TREE_CLASSIFIER
        hparam_tuning_objectives: ['F1_SCORE']
        num_trials: 10  # Number of trials for model hyper-parameter tuning
        category_encoding_method: ONE_HOT_ENCODING
        learn_rate: HPARAM_RANGE(0.00001, 1.0)
        l2_reg: HPARAM_RANGE(0, 10)
        max_tree_depth: HPARAM_RANGE(1, 16)
        subsample: HPARAM_RANGE(0.5, 1)
        max_iterations: 20
        early_stop: TRUE
        auto_class_weights: TRUE

training:
    cron: TZ=America/Los_Angeles 0 6 * * MON
    data_date_start_days_ago: 90  # How far back from today should we go to grab training data
    keep_n_best_models: 5  # How many models to keep saved (best model is always picked between retrains)

prediction:
    cron: TZ=America/Los_Angeles 0 11 * * *
    data_date_start_days_ago: 3  # How far back from today should we go to grab data for prediction

activation:
    ga4mp:  # GA4 Measurement Protocol based activation
        query_path: sp_ga4mp_activation.sqlx  # Path to the activation query
        ga4_measurement_id: G-J9D8V8M0V8
        ga4_property_id: 318895463
        ga4_api_secret: ZdUboRUkQFicOlE6DVPzT3
        ga4_mp_debug: True  # Use the prod GA4 MP endpoint or debug
        create_ga4_setup: False  # True means the conversion event and its params are created in GA4