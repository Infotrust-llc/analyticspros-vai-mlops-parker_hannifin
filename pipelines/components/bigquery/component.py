import os
from typing import List, Optional

from pipelines import config, base_image
from kfp.dsl import component, Output, Dataset, Model, Input, Metrics


@component(base_image=base_image)
def bq_call_create_dataset_op(
    project: str,
    run_id: str,
    dataset_id: str,
    p_mode: str,
    p_date_start: str,
    p_date_end: str,
    training_table: Output[Dataset],
    inference_table: Output[Dataset],
) -> None:
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY

    client = bigquery.Client(project=project)

    params = [
        bigquery.ScalarQueryParameter(
            "table_name", "STRING", f"{dataset_id}.{p_mode.lower()}_{run_id}"
        ),
        bigquery.ScalarQueryParameter("date_start", "DATE", p_date_start),
        bigquery.ScalarQueryParameter("date_end", "DATE", p_date_end),
        bigquery.ScalarQueryParameter("mode", "STRING", p_mode),
    ]

    job_config = bigquery.QueryJobConfig(
        query_parameters=params, labels={"vai-mlops": f"{p_mode.lower()}"}
    )

    query_job = client.query(
        query=f"""
        CALL `{project}.{dataset_id}.create_dataset`(@table_name, @date_start, @date_end, @mode);
        """,
        job_config=job_config,
        job_retry=BIGQUERY_RETRY_POLICY,
    )

    query_job.result()

    if p_mode == "TRAINING":
        training_table.metadata["table_id"] = (
            f"{project}.{dataset_id}." + f"{p_mode.lower()}_{run_id}"
        )

    if p_mode == "INFERENCE":
        inference_table.metadata["table_id"] = (
            f"{project}.{dataset_id}." + f"{p_mode.lower()}_{run_id}"
        )


@component(base_image=base_image)
def bqml_training_op(
    project: str,
    run_id: str,
    dataset_id: str,
    training_table: Input[Dataset],
    create_model_params: dict,
    model: Output[Model],
) -> None:
    from jinja2 import Environment, BaseLoader
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY

    from google.api_core.future.polling import DEFAULT_POLLING

    DEFAULT_POLLING._timeout = 60 * 60 * 24 * 3

    client = bigquery.Client(project=project)

    model_name = f"model_{run_id}"
    mt_is_automl = "AUTOML" in create_model_params["model_type"].upper()
    if mt_is_automl:  # remove two default params otherwise needed
        create_model_params.pop("hparam_tuning_objectives", None)
        create_model_params.pop("num_trials", None)

    sqlx = """
        CREATE OR REPLACE MODEL `{{ dataset_id }}.{{ model_name }}` 
            {% if create_model_params.transform is defined() %}
            {{ create_model_params.transform }}
            {% endif %}
            OPTIONS (
              {% for k, v in create_model_params.items() %}
              {% if k != "transform" %}
              {{ k }}=
                {% if (
                    v is string() 
                    and (
                        v.lower().startswith('hparam_') 
                        or v.lower().startswith('struct')
                        )
                    )
                    or v is not string() %}
                  {{ v }}
                {% else %}
                  '{{ v }}'
                {% endif %},
              {% endif %}
              {% endfor %}
              
              {% if not mt_is_automl %}
              DATA_SPLIT_METHOD='CUSTOM',
              HPARAM_TUNING_ALGORITHM='VIZIER_DEFAULT',
              {% endif %}
              DATA_SPLIT_COL='data_split',
              
              MODEL_REGISTRY='VERTEX_AI', 
              VERTEX_AI_MODEL_ID='{{ dataset_id }}' ) 
            AS (
                SELECT * FROM `{{ training_table_id }}`
            )"""
    tpl = Environment(loader=BaseLoader).from_string(sqlx)
    query = tpl.render(
        {
            "dataset_id": dataset_id,
            "model_name": model_name,
            "training_table_id": training_table.metadata["table_id"],
            "create_model_params": create_model_params,
            "mt_is_automl": mt_is_automl,
        }
    )

    job_config = bigquery.QueryJobConfig(labels={"vai-mlops": f"training"})
    query_job = client.query(
        query=query, job_config=job_config, job_retry=BIGQUERY_RETRY_POLICY
    )

    r = query_job.result()

    # save the Vertex Model ID and Version as labels in BQ
    bq_model = client.get_model(f"{project}.{dataset_id}.{model_name}")
    bq_model.labels = {
        # "vertexAiModelId": bq_model.training_runs[0]["vertexAiModelId"],
        "vertexAiModelVersion": bq_model.training_runs[0]["vertexAiModelVersion"],
    }
    client.update_model(bq_model, ["labels"])

    model.metadata = {
        "project_id": project,
        "dataset_id": dataset_id,
        "model_name": model_name,
        "model_id": f"{project}.{dataset_id}.{model_name}",
        "vertex_model_name": dataset_id,
    }


@component(base_image=base_image)
def bqml_list_models_op(project: str, dataset_id: str) -> List[dict]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)

    bqml_models = client.list_models(dataset_id)

    models = []
    for bqm in bqml_models:
        model = {
            "project_id": bqm.project,
            "dataset_id": bqm.dataset_id,
            "model_name": bqm.model_id,
            "model_id": f"{bqm.project}.{bqm.dataset_id}.{bqm.model_id}",
            "vertex_model_name": dataset_id,
        }
        models.append(model)

    return models


@component(base_image=base_image)
def bqml_model_evaluate_op(
    project: str,
    run_id: str,
    model: dict,
    training_table: Input[Dataset],
    metrics: Output[Metrics],
):
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY
    from google.cloud.exceptions import NotFound

    model_ = Model()
    model_.metadata.update(model)
    model = model_

    client = bigquery.Client(project=project)
    model_eval_table_id = f"{model.metadata['dataset_id']}.model_evals"

    q_statement = None
    try:
        client.get_table(model_eval_table_id)
        q_statement = f"""
        DELETE FROM `{model_eval_table_id}` 
        WHERE training_run_id = "{run_id}" AND model_name = "{model.metadata["model_name"]}";
        
        INSERT INTO `{model_eval_table_id}`
        """
    except NotFound:
        q_statement = f"""
        CREATE OR REPLACE TABLE `{model_eval_table_id}` 
        OPTIONS( labels = [("vai-mlops", "training")] )
        AS"""

    query = f"""
        {q_statement}
        SELECT 
            "{run_id}" as training_run_id, 
            "{model.metadata["model_name"]}" as model_name,
            * -- leave whatever the default output is
        FROM ML.EVALUATE(
            MODEL `{model.metadata["model_id"]}`,
            (
                SELECT * EXCEPT(data_split) FROM `{training_table.metadata['table_id']}`
                WHERE data_split = 'TEST'
            )
        );
        """

    job_config = bigquery.QueryJobConfig(labels={"vai-mlops": f"training"})
    client.query(
        query=query, job_config=job_config, job_retry=BIGQUERY_RETRY_POLICY
    ).result()

    query_job = client.query(
        query=f"""
        SELECT * FROM `{model_eval_table_id}`
        WHERE training_run_id = "{run_id}" AND model_name = "{model.metadata["model_name"]}"
        """,
        job_retry=BIGQUERY_RETRY_POLICY,
    )

    r = query_job.result()
    r = list(r)

    for i in r:
        for k, v in i.items():
            metrics.log_metric(k, v)


@component(base_image=base_image)
def bqml_model_cleanup_op(
    project: str,
    dataset_id: str,
    keep_n_best_models: Optional[int],
    eval_metrics: Optional[List[Metrics]],
    best_model: Output[Model],
    metrics: Output[Metrics],
):
    import logging
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY
    from google.cloud import aiplatform

    def _metric_min_max(m):
        if m in (
            "mean_squared_error",
            "mean_absolute_error",
            "mean_squared_log_error",
            "log_loss",
        ):
            return "ASC"
        if m in ("roc_auc", "f1_score", "recall", "precision"):
            return "DESC"

    client = bigquery.Client(project=project)

    model_latest = sorted(
        list(client.list_models(dataset_id)),
        key=lambda m: m.created,
        reverse=True,
    )[0]
    model_latest = client.get_model(model_latest.reference)
    eval_metric = None

    # ML.EVALUATE output https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-evaluate#output
    if (
        "AUTOML" in model_latest.model_type.upper()
    ):  # https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-automl#optimization_objective
        em = model_latest.training_runs[0]["trainingOptions"].get(
            "optimizationObjective", None
        )
        if em is None and model_latest.model_type == "AUTOML_REGRESSOR":
            eval_metric = "mean_squared_error"
        elif em is None and model_latest.model_type == "AUTOML_CLASSIFIER":
            eval_metric = "roc_auc"
        elif isinstance(em, str):
            # regression
            if em.upper() == "MINIMIZE_RMSE":
                eval_metric = "mean_squared_error"
            elif em.upper() == "MINIMIZE_MAE":
                eval_metric = "mean_absolute_error"
            elif em.upper() == "MINIMIZE_RMSLE":
                eval_metric = "mean_squared_log_error"
            # classification
            elif em.upper() == "MAXIMIZE_AU_ROC":
                eval_metric = "roc_auc"
            elif em.upper() == "MINIMIZE_LOG_LOSS":
                eval_metric = "log_loss"
            elif em.upper() == "MAXIMIZE_AU_PRC":
                # au_prc doesn't seem to exist in the ML.EVALUATION output, mapping to f1_score
                eval_metric = "f1_score"
        elif isinstance(em, tuple):
            if em[0].upper() == "MAXIMIZE_PRECISION_AT_RECALL":
                eval_metric = "recall"
            elif em[0].upper() == "MAXIMIZE_RECALL_AT_PRECISION":
                eval_metric = "precision"

    else:
        eval_metric = model_latest.training_runs[0]["trainingOptions"][
            "hparamTuningObjectives"
        ][0].lower()

    query = f"""
        SELECT
          model_name, {eval_metric} as eval_metric
        FROM `{project}.{dataset_id}.model_evals`
        WHERE TRUE
        QUALIFY 
          MAX(training_run_id) OVER () = training_run_id
        ORDER BY {eval_metric} {_metric_min_max(eval_metric)}
    """

    job_config = bigquery.QueryJobConfig(labels={"vai-mlops": f"training"})
    query_job = client.query(
        query=query, job_config=job_config, job_retry=BIGQUERY_RETRY_POLICY
    )
    r = query_job.result()
    r = list(r)

    best_model.metadata = {
        "project_id": project,
        "dataset_id": dataset_id,
        "model_name": r[0]["model_name"],
        "model_id": f"{project}.{dataset_id}.{r[0]['model_name']}",
        "vertex_model_name": dataset_id,
    }
    metrics.log_metric(eval_metric, r[0]["eval_metric"])

    # set best model as default in VAI Model Registry
    bq_model = client.get_model(f"{project}.{dataset_id}.{r[0]['model_name']}")
    vai_model = aiplatform.ModelRegistry(
        model=bq_model.training_runs[0]["vertexAiModelId"]
    )
    vai_model.add_version_aliases(
        ["default"], bq_model.training_runs[0]["vertexAiModelVersion"]
    )

    if keep_n_best_models is None or keep_n_best_models <= 1:
        return

    # delete models outside of top keep_n_best
    if (
        len(r) <= keep_n_best_models
    ):  # don't delete anything, we don't have too many models
        return

    for m in r[keep_n_best_models:]:
        model_id = f"{project}.{dataset_id}.{m['model_name']}"
        logging.info(f"Deleting model: `{model_id}`")
        client.delete_model(model_id)


@component(base_image=base_image)
def bq_calc_percentile_map_op(
    project: str,
    dataset_id: str,
    run_id: str,
    training_table: Input[Dataset],
    model: Input[Model],
    percentile_map_table: Output[Dataset],
):
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY
    from google.cloud.exceptions import NotFound

    client = bigquery.Client(project=project)

    bq_model = client.get_model(model.metadata["model_id"])
    if "REGRESSOR" in bq_model.model_type.upper():
        ml_type = "REGRESSOR"

    elif "CLASSIFIER" in bq_model.model_type.upper():
        ml_type = "CLASSIFIER"

    else:
        return

    perc_map_table_id = f"{dataset_id}.model_percentile_map"

    q_statement = None
    try:
        client.get_table(perc_map_table_id)
        q_statement = f"""
        DELETE FROM `{perc_map_table_id}` 
        WHERE training_run_id = "{run_id}" AND model_name = "{model.metadata["model_name"]}";

        INSERT INTO `{perc_map_table_id}`
        """
    except NotFound:
        q_statement = f"""
        CREATE OR REPLACE TABLE `{perc_map_table_id}` 
        OPTIONS( labels = [("vai-mlops", "training")] ) 
        AS"""

    query = f"""
    {q_statement}
    WITH 
      predictions AS (
        SELECT 
          {
            "predicted_label as p" 
            if ml_type == "REGRESSOR" 
            else 
                "(SELECT pl.prob FROM UNNEST(predicted_label_probs) as pl WHERE pl.label = 1 LIMIT 1) as p"
          }
          ,
          label
        FROM ML.PREDICT(MODEL `{model.metadata['model_id']}`, 
          (
            SELECT * FROM `{training_table.metadata['table_id']}` WHERE data_split = 'TEST'
          )
        )
      ),
    
      ptiles AS (
         SELECT 
          APPROX_QUANTILES(p , 99) as tile
        FROM predictions
      )
    
    SELECT
      training_run_id, model_name,
      percentile, lower_bnd, upper_bnd,
      IFNULL(SUM(label) / COUNT(*), 0) as conv_rate,
      COUNT(*) / SUM(COUNT(*)) OVER() as size_share,
      SUM(SUM(label)) OVER () / SUM(COUNT(*)) OVER() as conv_rate_base
        
    FROM (
      SELECT
        "{run_id}" as training_run_id, 
        "{model.metadata['model_name']}" as model_name,
        *
      FROM (
        SELECT
          q - 1 as percentile,
          IF(prev_lower_bnd IS NULL, CAST('-inf' as FLOAT64), lower_bnd) as lower_bnd,
          IF(upper_bnd IS NULL, CAST('inf' as FLOAT64), upper_bnd) as upper_bnd
        FROM (
          SELECT
            ROW_NUMBER() OVER (ORDER BY tile) as q,
            LAG(tile) OVER (ORDER BY tile) as prev_lower_bnd,
            tile as lower_bnd,
            LEAD(tile) OVER (ORDER BY tile) as upper_bnd,
            
    
          FROM ptiles, UNNEST(tile) as tile
        )
      )
      WHERE lower_bnd != upper_bnd
    ) as b
    LEFT JOIN predictions as p
      ON p.p >= b.lower_bnd AND p.p < b.upper_bnd
    GROUP BY 1,2,3,4,5
    """

    job_config = bigquery.QueryJobConfig(labels={"vai-mlops": f"training"})
    query_job = client.query(
        query=query, job_config=job_config, job_retry=BIGQUERY_RETRY_POLICY
    )
    query_job.result()

    percentile_map_table.metadata["table_id"] = perc_map_table_id


@component(base_image=base_image)
def bqml_predict_op(
    project: str,
    dataset_id: str,
    run_id: str,
    inference_table: Input[Dataset],
    model: Input[Model],
    predictions_table: Output[Dataset],
):
    from google.cloud import bigquery
    from common.retry_policies import BIGQUERY_RETRY_POLICY
    from google.cloud.exceptions import NotFound

    client = bigquery.Client(project=project)

    predictions_table_id = f"{project}.{dataset_id}.predictions"

    q_statement = None
    try:
        client.get_table(predictions_table_id)
        q_statement = f"""
        DELETE FROM `{predictions_table_id}` 
        WHERE prediction_run_id = "{run_id}" AND model_name = "{model.metadata["model_name"]}";

        INSERT INTO `{predictions_table_id}`
        """
    except NotFound:
        q_statement = f"""
        CREATE OR REPLACE TABLE `{predictions_table_id}` 
        PARTITION BY date
        CLUSTER BY prediction_run_id
        OPTIONS( labels = [("vai-mlops", "inference")] )
        AS"""

    query = f"""
        CREATE OR REPLACE TEMP TABLE temp_predictions AS
        SELECT
          "{run_id}" as prediction_run_id,
          "{model.metadata["model_name"]}" as model_name,
          * EXCEPT(predicted_label) 
        FROM ML.PREDICT(
          MODEL `{model.metadata['model_id']}`, 
          TABLE `{inference_table.metadata['table_id']}`
        );
    
        {q_statement}
        SELECT * FROM temp_predictions;
    """

    job_config = bigquery.QueryJobConfig(labels={"vai-mlops": f"inference"})
    query_job = client.query(
        query=query, job_config=job_config, job_retry=BIGQUERY_RETRY_POLICY
    )
    query_job.result()

    predictions_table.metadata["table_id"] = predictions_table_id
