

-- SQL-Bigquery görev-3
 
with session_data as (
  SELECT
    user_pseudo_id,
    concat(user_pseudo_id,(SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id')) as user_session_id,
    regexp_extract((SELECT value.string_value FROM unnest(event_params) where key = 'page_location'), r'https://[^\/]+/([^7#]*)') AS page_path
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name = 'session_start'
    and _TABLE_SUFFIX between '20200101' and '20201231'
),
purchase_data as (
  SELECT
    user_pseudo_id,
    concat(user_pseudo_id,(SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id')) as user_session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name = 'purchase'
    and _TABLE_SUFFIX between '20200101' and '20201231'
),
combined_data as (
  SELECT
    s.page_path,
    s.user_pseudo_id,
    s.user_session_id,
    case when p.user_pseudo_id is not null then 1 else 0 end as has_purchase
  FROM session_data s
  LEFT JOIN purchase_data p
    ON s.user_pseudo_id = p.user_pseudo_id
    AND s.user_session_id = p.user_session_id
)
SELECT
  page_path,
  count(distinct user_session_id) as unique_sessions,
  sum(has_purchase) as total_purchases,
  round(sum(has_purchase) / count(distinct user_session_id), 3) as conversion_rate
FROM combined_data
group by page_path
order by total_purchases desc;

-- ***********************************************************************************************

-- SQL-Bigquery görev-4

with session_data as (
  SELECT
    concat(user_pseudo_id,(SELECT value.int_value FROM unnest(event_params) WHERE key = 'ga_session_id')) as user_session_id,
    max(case when key = 'session_engaged' and value.int_value = 1 then 1 else 0 end) as is_engaged,
    sum(case when key = 'engagement_time_msec' then value.int_value else 0 end) as total_engagement_time,
    max(case when event_name = 'purchase' then 1 else 0 end) as has_purchase

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`, unnest(event_params)
  WHERE _TABLE_SUFFIX between '20200101' and '20201231'
  group by user_session_id
)
SELECT
  round(corr(is_engaged, has_purchase),3) as correlation_engagement_purchase,
  round(corr(total_engagement_time, has_purchase),3) as correlation_time_purchase
FROM session_data;


