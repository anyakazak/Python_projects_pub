-- Data Preparation for Reporting in BI Systems
SELECT 
  event_timestamp,
  user_pseudo_id,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
  event_name,
  geo.country AS country,
  device.category,
  traffic_source.source AS source,
  traffic_source.medium AS medium,
  traffic_source.name AS campaign
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
AND event_name IN (
    'session_start',
    'view_item',
    'add_to_cart',
    'begin_checkout',
    'add_shipping_info',     
    'add_payment_info',      
    'purchase'               
  )
ORDER BY
  user_pseudo_id,
  session_id,
  event_timestamp ASC;

  -- Calculation of Conversions by Date and Traffic Channels 
  -- Variant 1 (simple, but long)

WITH EventData AS (       -- table of events that contains information about the date, user, session, event, and traffic source
  SELECT 
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    event_name,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  ),
UserSessionsCount AS (    -- table with the number of unique sessions of unique users for each date and traffic source
  SELECT
    event_date,
    source,
    medium,
    campaign,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '_', session_id)) AS user_sessions_count
  FROM EventData
  GROUP BY
    event_date,
    source,
    medium,
    campaign
),
CountUsersStart AS (      -- table with the number of unique users who started a session
  SELECT
    event_date,
    source,
    medium,
    campaign,
    COUNT(DISTINCT user_pseudo_id) AS users_start
  FROM EventData
  WHERE event_name = 'session_start'
  GROUP BY
    event_date,
    source,
    medium,
    campaign
),
CountUsersAddToCart AS (    -- table with the number of unique users who added a product to the cart
  SELECT
    event_date,
    source,
    medium,
    campaign,
    COUNT(DISTINCT user_pseudo_id) AS users_add_to_cart
  FROM EventData
  WHERE event_name = 'add_to_cart'
  GROUP BY
    event_date,
    source,
    medium,
    campaign
),
CountUsersBeginCheckout AS (   -- table with the number of unique users who began the checkout process
  SELECT
    event_date,
    source,
    medium,
    campaign,
    COUNT(DISTINCT user_pseudo_id) AS users_begin_checkout
  FROM EventData
  WHERE event_name = 'begin_checkout'
  GROUP BY
    event_date,
    source,
    medium,
    campaign
),
CountUsersPurchase AS (   -- table with the number of unique users who made a purchase
  SELECT
    event_date,
    source,
    medium,
    campaign,
    COUNT(DISTINCT user_pseudo_id) AS users_purchase
  FROM EventData
  WHERE event_name = 'purchase'
  GROUP BY
    event_date,
    source,
    medium,
    campaign
)
SELECT             -- combination of all the previous tables with conversion calculations for each date and traffic source
  usc.event_date,
  usc.source,
  usc.medium,
  usc.campaign,
  usc.user_sessions_count,
  ROUND((COALESCE(cuatc.users_add_to_cart, 0) / COALESCE(cus.users_start, 1)) * 100,2) AS visit_to_cart,
  ROUND((COALESCE(cubc.users_begin_checkout, 0) / COALESCE(cus.users_start, 1)) * 100,2) AS visit_to_checkout,
  ROUND((COALESCE(cup.users_purchase, 0) / COALESCE(cus.users_start, 1)) * 100,2) AS visit_to_purchase
FROM UserSessionsCount usc
LEFT JOIN CountUsersStart cus
  ON usc.event_date = cus.event_date
  AND usc.source = cus.source
  AND usc.medium = cus.medium
  AND usc.campaign = cus.campaign
LEFT JOIN CountUsersAddToCart cuatc
  ON usc.event_date = cuatc.event_date
  AND usc.source = cuatc.source
  AND usc.medium = cuatc.medium
  AND usc.campaign = cuatc.campaign
LEFT JOIN CountUsersBeginCheckout cubc
  ON usc.event_date = cubc.event_date
  AND usc.source = cubc.source
  AND usc.medium = cubc.medium
  AND usc.campaign = cubc.campaign
LEFT JOIN CountUsersPurchase cup
  ON usc.event_date = cup.event_date
  AND usc.source = cup.source
  AND usc.medium = cup.medium
  AND usc.campaign = cup.campaign
ORDER BY
  usc.event_date,
  usc.source,
  usc.medium,
  usc.campaign;


-- Variant 2 (optimized)
  WITH EventData AS (         -- table of events that contains information about the date, user, session, event, and traffic source
  SELECT 
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    event_name,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
),
EventCounts AS (             
--table with counts of sessions, users who started a session, added a product to the cart, began the checkout process, and made a purchase
  SELECT
    event_date,
    source,
    medium,
    campaign,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '_', session_id)) AS user_sessions_count,
    COUNT(DISTINCT CASE WHEN event_name = 'session_start' THEN user_pseudo_id ELSE NULL END) AS users_start,
    COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_pseudo_id ELSE NULL END) AS users_add_to_cart,
    COUNT(DISTINCT CASE WHEN event_name = 'begin_checkout' THEN user_pseudo_id ELSE NULL END) AS users_begin_checkout,
    COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id ELSE NULL END) AS users_purchase
  FROM EventData
  GROUP BY
    event_date,
    source,
    medium,
    campaign
)
SELECT
  event_date,
  source,
  medium,
  campaign,
  user_sessions_count,
  ROUND((COALESCE(users_add_to_cart, 0) / NULLIF(users_start, 0)) * 100, 2) AS visit_to_cart,
  ROUND((COALESCE(users_begin_checkout, 0) / NULLIF(users_start, 0)) * 100, 2) AS visit_to_checkout,
  ROUND((COALESCE(users_purchase, 0) / NULLIF(users_start, 0)) * 100, 2) AS visit_to_purchase
FROM EventCounts
ORDER BY
  event_date,
  source,
  medium,
  campaign;

--  Comparison of Conversions Between Different Landing Pages
  With SessionStartPageLocation AS(                          -- session start data and page_location
  SELECT
  user_pseudo_id,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
  WHERE _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
  AND event_name = 'session_start'
),
SessionStartPath AS(                                       -- extraction of page_path from page_location
  SELECT
    user_pseudo_id,
    session_id,
    page_location,
    REGEXP_EXTRACT(page_location, r'https?://[^/]+(/[^?]*)') AS page_path
  FROM SessionStartPageLocation
),
Purchases AS (                                             -- purchases associated with sessions
  SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE event_name = 'purchase' AND _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
)
SELECT
  s.page_path,
  COUNT(DISTINCT CONCAT(s.user_pseudo_id, '_', s.session_id )) AS unique_sessions,       -- number of unique sessions 
  COUNT(DISTINCT CONCAT(p.user_pseudo_id, '_', p.session_id )) AS purchases,             -- number of purchases
  ROUND (
    (COUNT(DISTINCT CONCAT(p.user_pseudo_id, '_', p.session_id ))/                     
    NULLIF(COUNT(DISTINCT CONCAT(s.user_pseudo_id, '_', s.session_id )), 0))*100, 2      -- conversion from session start to purchase
  ) AS conversion_rate
FROM SessionStartPath s
LEFT JOIN Purchases p
ON  s.user_pseudo_id = p.user_pseudo_id
AND s.session_id = p.session_id
GROUP BY s.page_path
ORDER BY conversion_rate DESC;

-- Correlation Check Between User Engagement and Purchases
WITH RawSessionData AS (
  SELECT
    user_pseudo_id,
    MAX(CASE WHEN params.key = 'ga_session_id' THEN params.value.int_value END) AS session_id,
    MAX(CASE WHEN params.key = 'session_engaged' AND params.value.string_value = '1' THEN 1 ELSE 0 END) AS engaged,
    SUM(CASE WHEN params.key = 'engagement_time_msec' THEN CAST(params.value.int_value AS INT64) ELSE 0 END) AS total_engagement_time,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS made_purchase
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  CROSS JOIN UNNEST(event_params) AS params
  GROUP BY user_pseudo_id, event_name
),
SessionData AS (
  SELECT
    user_pseudo_id,
    session_id,
    MAX(engaged) AS engaged,
    SUM(total_engagement_time) AS total_engagement_time,
    MAX(made_purchase) AS made_purchase
  FROM RawSessionData
  GROUP BY user_pseudo_id, session_id
),
EngagementPurchaseCorrelation AS (
  SELECT
    ROUND(CORR(engaged, made_purchase), 6) AS corr_engagement_vs_purchase,
    ROUND(CORR(total_engagement_time, made_purchase), 6) AS corr_engagement_time_vs_purchase
  FROM SessionData
)
SELECT *
FROM EngagementPurchaseCorrelation;
