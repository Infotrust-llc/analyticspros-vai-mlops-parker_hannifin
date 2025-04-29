CREATE OR REPLACE PROCEDURE `{{ billing_project_id }}.{{ dst_dataset }}.create_dataset`(
  table_name STRING,
  date_start DATE,
  date_end DATE,
  mode STRING
)
BEGIN

  DECLARE LOOKBACK_DAYS INT64 DEFAULT {{ lookback }};
  DECLARE LOOKAHEAD_DAYS INT64 DEFAULT {{ lookahead }};
  DECLARE RE_PAGE_PATH STRING DEFAULT "{{ re_page_path }}";

  CREATE OR REPLACE TEMP TABLE dataset AS (
    WITH
      visitor_pool AS (
      ----
      -- Should return only user_pseudo_ids that are eligible for the dataset creation.
      -- Potential exclusions: bounced sessions, visitors with only one session, must have more than 2 total pageviews
      ----
        SELECT
          user_pseudo_id,
          MAX(event_date) as last_event_date,
          MAX(event_tstamp) as last_event_tstamp,
          COUNTIF(
            event_name = 'page_view'
            AND event_tstamp BETWEEN TIMESTAMP_SUB(last_event_tstamp, INTERVAL LOOKBACK_DAYS DAY) AND last_event_tstamp
          ) as pageviews
        FROM (
          SELECT
            user_pseudo_id,
            event_name,
            SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) as event_date,
            TIMESTAMP_MICROS(event_timestamp) as event_tstamp,
            MAX(TIMESTAMP_MICROS(event_timestamp)) OVER (PARTITION BY user_pseudo_id) as last_event_tstamp,
          FROM
            `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
          WHERE
            SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN date_start - LOOKBACK_DAYS AND date_end
        )
        GROUP BY user_pseudo_id
        HAVING
          last_event_date BETWEEN date_start AND date_end  -- only keep visitors who's last visit is within the range
          AND pageviews > 1  -- only keep visitors who have more than 1 pageviews
      ),

      base_sessions AS (
      ----
      -- Creates session based metric for each eligible session
      ----
        SELECT
          ga.user_pseudo_id,
          session_id,
          MIN(SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) as date,
          MIN(TIMESTAMP_MICROS(event_timestamp)) as session_start_tstamp,
          MAX(TIMESTAMP_MICROS(event_timestamp)) as session_end_tstamp,
          TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) as time_on_site,
          COUNTIF(event_name = 'page_view') as pageviews,
          COUNTIF(event_name = 'view_item') as view_items,
          COUNTIF(event_name = 'view_promotion') as view_promotions,
          COUNTIF(event_name = 'view_search_results') as view_search_results,
          COUNTIF(event_name = 'add_to_cart') as add_to_carts,
          COUNTIF(event_name = 'begin_checkout') as begin_checkout,
          COUNTIF(event_name = 'purchase') as purchase,

          {% for key, value in features.iterrows() %}
          COUNTIF(page_path ="{{ value['feature'] }}") as {{ value["feature_name"] }}_visits,
          {% endfor %}

        FROM (
          SELECT
            user_pseudo_id,
            event_name,
            _TABLE_SUFFIX,
            event_timestamp,
            user_pseudo_id ||"/"||event_date||"/"|| (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id") as session_id,
            REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) as ep WHERE ep.key = 'page_location'), RE_PAGE_PATH) as page_path,
          FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
          WHERE SAFE.PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN date_start - LOOKBACK_DAYS AND date_end + LOOKAHEAD_DAYS
        ) as ga
        INNER JOIN visitor_pool as vp
          ON vp.user_pseudo_id = ga.user_pseudo_id
            AND TIMESTAMP_MICROS(ga.event_timestamp)
              BETWEEN
                TIMESTAMP_SUB(vp.last_event_tstamp, INTERVAL LOOKBACK_DAYS DAY)
                AND
                TIMESTAMP_ADD(vp.last_event_tstamp, INTERVAL LOOKAHEAD_DAYS DAY)
        GROUP BY
          1, 2
      ),

      conv AS (
      ----
      -- For each session returns the label
      ----
        SELECT
          bs1.user_pseudo_id,
          bs1.session_id,
          MIN(IF(bs2.purchase > 0, bs2.session_start_tstamp, NULL)) as conv_session_start_tstamp,
          IF(MAX(bs2.purchase) > 0, 1, 0) as label

        FROM base_sessions as bs1
        LEFT JOIN base_sessions as bs2
        ON
          bs2.user_pseudo_id = bs1.user_pseudo_id
          AND bs2.session_start_tstamp BETWEEN bs1.session_end_tstamp AND TIMESTAMP_ADD(bs1.session_end_tstamp, INTERVAL LOOKAHEAD_DAYS DAY)
        GROUP BY 1, 2
      ) 


    SELECT
      bs.user_pseudo_id, bs.session_id, bs.date, bs.session_start_tstamp, bs.session_end_tstamp,

      -- last session totals
      bs.time_on_site,
      bs.pageviews,
      bs.view_items,
      bs.view_promotions,
      bs.view_search_results,
      bs.add_to_carts,
      bs.begin_checkout,
      bs.purchase,

      {% for key, value in features.iterrows() %}
      bs.{{ value["feature_name"] }}_visits
      {% endfor %}

      -- totals
      COUNT(*) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as sessions,
      SUM(bs.time_on_site) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_time_on_site,
      SUM(bs.pageviews) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_pageviews,
      SUM(bs.view_items) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_view_items,
      SUM(bs.view_promotions) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_view_promotions,
      SUM(bs.view_search_results) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_view_search_results,
      SUM(bs.add_to_carts) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_add_to_carts,
      SUM(bs.begin_checkout) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_begin_checkout,
      SUM(bs.purchase) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_purchase,

      {% for key, value in features.iterrows() %}
      SUM(bs.{{ value["feature_name"] }}_visits) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC) as ttl_{{ value["feature_name"] }}_visits,
      {% endfor %} 

      IFNULL(
        TIMESTAMP_DIFF(
          bs.session_end_tstamp,
          MAX(IF(bs.purchase > 0, bs.session_end_tstamp, NULL)) OVER (PARTITION BY bs.user_pseudo_id ORDER BY bs.session_start_tstamp ASC),
          HOUR
        ),
        LOOKBACK_DAYS * 24  *2
      ) as last_purchase_hours_ago,

      c.label
    FROM base_sessions as bs
    INNER JOIN conv as c USING(session_id)
  );



  IF mode = 'TRAINING' THEN
    ----
    -- Training table should follow below rules:
    --     - must be named `table_name`
    --     - must have a `label` column which represents the label/target to train on
    --     - must have a `data_split` with values either TRAIN, EVAL, TEST
    --     - should only include feature columns and the label
    ----
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TABLE %s
      OPTIONS(
        expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY),
        labels=[("vai-mlops", "training")]
      )
      AS
      SELECT
        CASE
            WHEN MOD(ABS(FARM_FINGERPRINT(user_pseudo_id)), 100) < 70 THEN 'TRAIN'
            WHEN MOD(ABS(FARM_FINGERPRINT(user_pseudo_id)), 100) < 90 THEN 'EVAL'
            ELSE 'TEST'
        END as data_split,
        * EXCEPT (user_pseudo_id, session_id, date, session_start_tstamp, session_end_tstamp)
      FROM dataset
      WHERE
        date <= @de;
    """, table_name)
    USING date_end - LOOKAHEAD_DAYS as de;

  END IF;

  IF mode = 'INFERENCE' THEN
      ----
      -- Inference table should follow below rules:
      --     - must be named `table_name`
      --     - must have a `date` column of type DATE (later used for table partitioning)
      --     - should include all feature columns
      --     - can include any other relevant column
      ----
      EXECUTE IMMEDIATE FORMAT("""
        CREATE OR REPLACE TABLE %s
        OPTIONS(
          expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY),
          labels=[("vai-mlops", "inference")]
        )
        AS
        SELECT
          * EXCEPT (label)
        FROM dataset
        WHERE date BETWEEN @ds AND @de;
    """, table_name)
    USING date_start as ds, date_end as de;

  END IF;

  DROP TABLE dataset;

END;
