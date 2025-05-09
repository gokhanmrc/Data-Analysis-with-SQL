WITH new_all_tables AS (
    SELECT 
        ad_date,
        'facebook ads' AS media_source,
        campaign_name, 
        adset_name, 
        spend, 
        impressions, 
        reach, 
        clicks, 
        leads, 
        value
    FROM homeworks.facebook_ads_basic_daily AS fabd
    LEFT JOIN public.facebook_adset AS fa ON fabd.adset_id = fa.adset_id
    LEFT JOIN public.facebook_campaign AS fc ON fabd.campaign_id = fc.campaign_id
    UNION ALL
    SELECT 
        ad_date,
        'google ads' AS media_source,
        campaign_name, 
        adset_name, 
        spend, 
        impressions, 
        reach, 
        clicks, 
        leads, 
        value
    FROM public.google_ads_basic_daily
), ek_gorev AS ( 
SELECT 
    --ad_date, 
    media_source, 
    campaign_name, 
    adset_name,
    SUM(spend) AS toplam_maliyet,
    SUM(impressions) AS toplam_gosterim,
    SUM(clicks) AS toplam_tiklama,
    SUM(value) AS toplam_donusum_degeri,
    CASE 
        WHEN SUM(spend) = 0 THEN NULL
        ELSE (SUM(value) :: numeric - SUM(spend):: numeric) / SUM(spend) :: numeric* 100
    END AS romi
FROM new_all_tables
GROUP BY --ad_date, 
		 media_source, 
		 campaign_name, 
		 adset_name
		 )
SELECT*
FROM ek_gorev
WHERE toplam_maliyet > 500000
ORDER BY toplam_maliyet desc
LIMIT 1;