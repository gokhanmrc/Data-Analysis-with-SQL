WITH facebook_data AS (
    SELECT 
        a.ad_date, 
        'Facebook Ads' AS media_source,  -- Google tablosu için kaynak adı
        c.campaign_name, 
        b.adset_name, 
        a.spend, 
        a.impressions, 
        a.reach, 
        a.clicks, 
        a.leads, 
        a.value
    FROM public.facebook_ads_basic_daily a
    JOIN public.facebook_adset b ON a.adset_id = b.adset_id
    JOIN public.facebook_campaign c ON a.campaign_id = c.campaign_id
    ),
    Google_data AS (
    
    SELECT 
        ad_date, 
        'Google Ads' AS media_source,  -- Google tablosu için kaynak adı
        campaign_name, 
        adset_name, 
        spend, 
        impressions, 
        reach, 
        clicks, 
        leads, 
        value
    FROM public.google_ads_basic_daily
    ) 
SELECT 
    ad_date,
    media_source,
    campaign_name,
    adset_name,
    SUM(spend) AS total_spend,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(value) AS total_value
FROM (
    SELECT * FROM facebook_data
    UNION ALL
    SELECT * FROM google_data
) AS combined_data
GROUP BY 
    ad_date, 
    media_source, 
    campaign_name, 
    adset_name;