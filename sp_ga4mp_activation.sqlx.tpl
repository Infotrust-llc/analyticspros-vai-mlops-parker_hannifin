WITH
  p_map AS (  -- produces the latest proba to percentile map for each model
    SELECT
      * EXCEPT(training_run_id)
    FROM `{{ gcp_project_id }}.{{ bq_dataset_id }}.model_percentile_map`
    WHERE TRUE
    QUALIFY MAX(training_run_id) OVER (PARTITION BY model_name) = training_run_id
  ),

  p_latest AS (  -- gets all predictions for the latest prediction run for the latest session by client_id
    SELECT
      p.*,
      p_map.percentile
    FROM (
      SELECT
        date,
        prediction_run_id,
        model_name,
        session_id,

        user_pseudo_id as client_id,
        session_end_tstamp as event_timestamp,
        (SELECT plp.prob FROM UNNEST(predicted_label_probs) as plp WHERE plp.label = 1) as proba

      FROM `{{ gcp_project_id }}.{{ bq_dataset_id }}.predictions` as p
      WHERE date >= CURRENT_DATE() - 7
      QUALIFY MAX(prediction_run_id) OVER() = prediction_run_id
    ) as p
    INNER JOIN p_map
      ON p_map.model_name = p.model_name
          AND p.proba >= p_map.lower_bnd AND p.proba < p_map.upper_bnd
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY p.event_timestamp DESC) = 1
  ),

  p_before_latest AS (  -- gets the latest predictions for each latest session before the last prediction run
    SELECT
      p.*,
      p_map.percentile
    FROM (
      SELECT
        date,
        prediction_run_id,
        model_name,
        session_id,

        user_pseudo_id as client_id,
        session_end_tstamp as event_timestamp,
        (SELECT plp.prob FROM UNNEST(predicted_label_probs) as plp WHERE plp.label = 1) as proba

      FROM `{{ gcp_project_id }}.{{ bq_dataset_id }}.predictions` as p
      WHERE date >= CURRENT_DATE() - 7
      QUALIFY MAX(prediction_run_id) OVER() != prediction_run_id
    ) as p
    INNER JOIN p_map
      ON p_map.model_name = p.model_name
          AND p.proba >= p_map.lower_bnd AND p.proba < p_map.upper_bnd
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY p.event_timestamp DESC) = 1
  )

  SELECT
    pl.client_id,
    '{{ bq_dataset_id }}' as  event_name,
    pl.event_timestamp,
    pl.percentile as up_percentile,
    ROUND(100 * pl.proba, 1) as up_prediction,

#    ROUND(100 * pl.proba, 1) as ep_value_,  # for GAds conversion event integration
#    'USD' as ep_currency_  # for GAds conversion event integration

  FROM p_latest as pl
  LEFT JOIN p_before_latest as pbl USING (client_id)
  WHERE
    pbl.percentile IS NULL  -- if prior prediction doesn't exist we need to pass to GA4
    OR pbl.percentile != pl.percentile  -- if prior prediction is different than current we need to pass to GA4
