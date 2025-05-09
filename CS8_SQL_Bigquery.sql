-- SQL-Bigquery görev-1 (https://console.cloud.google.com/bigquery?sq=996353197838:242ab6b6a2e04cf8876386770b217d90)


SELECT 
  timestamp_micros(event_timestamp) as event_date,
  user_pseudo_id,
  (SELECT value.int_value FROM e.event_params WHERE key = 'ga_session_id') as session_id,
  event_name,
  geo.country,
  device.category,
  traffic_source.source,
  traffic_source.medium,
  traffic_source.name as campaign
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as e
  WHERE event_name IN (
        'session_start',
        'view_item',
        'add_to_cart',
        'begin_checkout',
        'add_shipping_info',
        'add_payment_info',
        'purchase')
    and _table_suffix between '20210101' and '20211231'
    
-- ***********************************************************************************************

-- SQL-Bigquery görev-2 (https://console.cloud.google.com/bigquery?sq=996353197838:00eeab56ff7845f594267fb01a53a534)

with new_table as (
  SELECT 
  date(timestamp_micros(event_timestamp)) as event_date,
  traffic_source.source as source,
  traffic_source.medium as medium,
  traffic_source.name as campaign,
  count(concat(user_pseudo_id,(SELECT value.int_value FROM e.event_params WHERE key = 'ga_session_id'))) as user_sessions_count,
  sum(case when event_name = 'add_to_cart' then 1 else 0 end) as atc_count,
  sum(case when event_name = 'begin_checkout' then 1 else 0 end) as bc_count,
  sum(case when event_name = 'purchase' then 1 else 0 end) as p_count,
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as e
  WHERE _table_suffix between '20210101' and '20211231'
  group by event_date,source,medium,campaign
)SELECT
    event_date,
    source,
    medium,
    campaign,
    user_sessions_count,
    round(SAFE_DIVIDE(atc_count,user_sessions_count),5) as visit_to_cart,
    round(SAFE_DIVIDE(bc_count,user_sessions_count),5) as visit_to_checkout,
    round(SAFE_DIVIDE(p_count,user_sessions_count),5) as Visit_to_purchase
  FROM new_table


