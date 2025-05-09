WITH combined_ads_data AS (
    SELECT 
        ad_date, 
        'Facebook Ads' AS media_source,  -- Facebook tablosu için kaynak adı
        spend, 
        impressions, 
        reach, 
        clicks, 
        leads, 
        value
    FROM public.facebook_ads_basic_daily
    
    UNION ALL
    
    SELECT 
        ad_date, 
        'Google Ads' AS media_source,  -- Google tablosu için kaynak adı
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
    SUM(spend) AS total_spend,  -- Toplam harcama
    SUM(impressions) AS total_impressions,  -- Toplam gösterim sayısı
    SUM(clicks) AS total_clicks,  -- Toplam tıklama sayısı
    SUM(value) AS total_conversion_value  -- Toplam dönüşüm değeri
 FROM combined_ads_data
 GROUP BY ad_date, media_source;

